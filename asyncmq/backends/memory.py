import time
from typing import Any, cast

import anyio

from asyncmq.backends.base import (
    BaseBackend,
    DelayedInfo,
    RepeatableInfo,
    WorkerInfo,
)
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.core.inspection import matches_job_type, normalize_job_type
from asyncmq.core.repeatables import normalize_repeatable_job_def, repeatable_identity
from asyncmq.schedulers import compute_next_run

_JobKey = tuple[str, str]


class InMemoryBackend(BaseBackend):
    """
    An in-memory implementation of the AsyncMQ backend.

    This backend stores all queue data, job states, results, and dependencies
    directly in Python dictionaries and lists in memory. It is suitable for
    testing, development, and simple use cases where persistence across process
    restarts is not required. It uses an anyio Lock for thread-safe access
    to its internal data structures within a single process.
    """

    def __init__(self) -> None:
        """
        Initializes the in-memory backend with empty data structures.
        """
        # Waiting queues: queue_name -> list of job payloads
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # Dead-letter queues: queue_name -> list of failed job payloads
        self.dlqs: dict[str, list[dict[str, Any]]] = {}
        # Delayed jobs: queue_name -> list of (run_at_ts, payload)
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        # Repeatable definitions: queue_name -> { repeatable_id -> { job_def, next_run, paused } }
        self.repeatables: dict[str, dict[str, dict[str, Any]]] = {}
        # Cancelled jobs: queue_name -> set of job_ids
        self.cancelled: dict[str, set[str]] = {}

        # Job state & result tracking
        self.job_states: dict[_JobKey, str] = {}
        self.job_results: dict[_JobKey, Any] = {}
        self.job_progress: dict[_JobKey, float] = {}
        self.job_payloads: dict[_JobKey, dict[str, Any]] = {}

        # Dependency tracking (existing features)
        self.deps_pending: dict[_JobKey, set[str]] = {}
        self.deps_children: dict[_JobKey, set[str]] = {}

        # Paused queues (existing feature)
        self.paused: set[str] = set()

        # Heartbeats & active jobs (existing features)
        self.heartbeats: dict[_JobKey, float] = {}
        self.active_jobs: dict[_JobKey, dict[str, Any]] = {}

        # Workers
        self._worker_registry: dict[str, WorkerInfo] = {}
        self._locks: dict[str, anyio.Lock] = {}

        # anyio Lock for thread-safe in-process synchronization
        self.lock = anyio.Lock()

    async def pop_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Atomically fetch and remove all delayed jobs whose run_at ≤ now.
        The existing get_due_delayed() already does this under the lock,
        so we can simply delegate to it.
        """
        return await self.get_due_delayed(queue_name)

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
        """
        Asynchronously enqueues a job payload onto the specified queue.

        Adds the job payload to the in-memory list for the queue and updates
        its state to WAITING. The queue is sorted by job priority after adding.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: A dictionary containing the job data, including an 'id'
                     and optionally a 'priority'.
        """
        async with self.lock:
            # Get or create the list for the queue.
            queue = self.queues.setdefault(queue_name, [])
            # Append the new job payload to the list.
            queue.append(payload)
            # Sort the queue based on job priority (lower number is higher priority).
            queue.sort(key=lambda job: job.get("priority", 5))
            # Update the job's state to WAITING.
            self.job_states[(queue_name, payload["id"])] = State.WAITING
            self.job_payloads[(queue_name, payload["id"])] = {**payload, "status": State.WAITING}
            return cast(str, payload["id"])

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Removes and returns the first job from the in-memory queue list if
        available. Does not update job state here; state transition to ACTIVE
        is handled by the worker.

        Args:
            queue_name: The name of the queue to dequeue from.

        Returns:
            The job dictionary if a job was available, otherwise None.
        """
        async with self.lock:
            # Get the list for the queue, defaulting to empty.
            queue = self.queues.get(queue_name, [])
            # If the queue is not empty, pop and return the first job.
            if queue:
                job = queue.pop(0)
                self.active_jobs[(queue_name, job["id"])] = job
                return job
            # Return None if the queue is empty.
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a failed job to the Dead Letter Queue (DLQ).

        Adds the job payload to the in-memory DLQ list for the queue and updates
        its state to FAILED.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: A dictionary containing the job data, including an 'id'.
        """
        async with self.lock:
            job_id = payload.get("id")
            if job_id is not None:
                self.queues[queue_name] = [j for j in self.queues.get(queue_name, []) if j.get("id") != job_id]
                self.delayed[queue_name] = [
                    (run_at, j) for run_at, j in self.delayed.get(queue_name, []) if j.get("id") != job_id
                ]
                self.active_jobs.pop((queue_name, str(job_id)), None)
                self.heartbeats.pop((queue_name, str(job_id)), None)

            # Get or create the list for the DLQ.
            dlq = self.dlqs.setdefault(queue_name, [])
            # Append the failed job payload to the DLQ list.
            dlq.append(payload)
            # Preserve explicit terminal status (e.g. EXPIRED) when provided.
            target_status = payload.get("status")
            if target_status not in {State.FAILED, State.EXPIRED}:
                target_status = State.FAILED
            self.job_states[(queue_name, payload["id"])] = target_status
            now = time.time()
            self.job_payloads[(queue_name, payload["id"])] = {
                **payload,
                "status": target_status,
                "updated_at": now,
                "failed_at": now,
            }

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Acknowledges successful processing of a job.

        This method is a no-op in this in-memory backend as state transitions
        (like completion) are handled by `update_job_state`.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The ID of the acknowledged job.
        """
        self.heartbeats.pop((queue_name, job_id), None)
        self.active_jobs.pop((queue_name, job_id), None)

    def _remove_job_memberships_locked(self, queue_name: str, job_id: str) -> None:
        self.queues[queue_name] = [
            job for job in self.queues.get(queue_name, []) if str(job.get("id")) != job_id
        ]
        self.delayed[queue_name] = [
            (run_at, job)
            for run_at, job in self.delayed.get(queue_name, [])
            if str(job.get("id")) != job_id
        ]
        self.active_jobs.pop((queue_name, job_id), None)
        self.heartbeats.pop((queue_name, job_id), None)

    def _store_lifecycle_payload_locked(
        self,
        queue_name: str,
        payload: dict[str, Any],
        status: str,
        now: float,
    ) -> dict[str, Any]:
        job_id = str(payload["id"])
        stored = {**payload, "status": status, "updated_at": now}
        if status == State.COMPLETED:
            stored["completed_at"] = now
        elif status in {State.FAILED, State.EXPIRED}:
            stored["failed_at"] = now

        self.job_states[(queue_name, job_id)] = status
        self.job_payloads[(queue_name, job_id)] = stored
        return stored

    async def complete_active_job(self, queue_name: str, payload: dict[str, Any], result: Any) -> None:
        job_id = str(payload["id"])
        now = time.time()
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            stored = self._store_lifecycle_payload_locked(
                queue_name,
                {**payload, "result": result},
                State.COMPLETED,
                now,
            )
            self.job_results[(queue_name, job_id)] = result
            self.job_payloads[(queue_name, job_id)] = stored

    async def retry_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at)

    async def defer_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at)

    async def _delay_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        job_id = str(payload["id"])
        now = time.time()
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            stored = self._store_lifecycle_payload_locked(
                queue_name,
                {**payload, "delay_until": run_at},
                State.DELAYED,
                now,
            )
            self.delayed.setdefault(queue_name, []).append((run_at, stored))

    async def fail_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        await self._terminal_dlq_transition(queue_name, payload, State.FAILED)

    async def expire_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        await self._terminal_dlq_transition(queue_name, payload, State.EXPIRED)

    async def _terminal_dlq_transition(self, queue_name: str, payload: dict[str, Any], status: str) -> None:
        job_id = str(payload["id"])
        now = time.time()
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            stored = self._store_lifecycle_payload_locked(queue_name, payload, status, now)
            self.dlqs.setdefault(queue_name, []).append(stored)

    async def cancel_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        job_id = str(payload["id"])
        async with self.lock:
            self.active_jobs.pop((queue_name, job_id), None)
            self.heartbeats.pop((queue_name, job_id), None)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time.

        Adds the job payload and its scheduled run time to the in-memory delayed
        list for the queue and updates the job's state to EXPIRED.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: A dictionary containing the job data, including an 'id'.
            run_at: The absolute timestamp (float) when the job should become
                    available for processing.
        """
        async with self.lock:
            # Get or create the list for delayed jobs.
            delayed_list = self.delayed.setdefault(queue_name, [])
            # Append the run_at timestamp and job payload as a tuple.
            delayed_list.append((run_at, payload))
            # Update the job's state to DELAYED.
            self.job_states[(queue_name, payload["id"])] = State.DELAYED
            self.job_payloads[(queue_name, payload["id"])] = {
                **payload,
                "status": State.DELAYED,
                "delay_until": run_at,
                "updated_at": time.time(),
            }

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed jobs that are now due for processing.

        Checks the in-memory delayed list for jobs whose scheduled run time
        is less than or equal to the current time. Removes due jobs from the
        delayed list and returns their payloads.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of job dictionaries that are ready to be processed.
        """
        async with self.lock:
            # Get the current time.
            now = time.time()
            due_jobs = []  # list to hold jobs that are due.
            still_delayed = []  # list to hold jobs that are not yet due.
            # Iterate through the delayed jobs for the queue.
            for run_at, payload in self.delayed.get(queue_name, []):
                # Check if the job's run time is now or in the past.
                if run_at <= now:
                    due_jobs.append(payload)  # Add due jobs to the 'due_jobs' list.
                else:
                    still_delayed.append((run_at, payload))  # Add remaining jobs to 'still_delayed'.
            # Update the in-memory delayed list with only the remaining jobs.
            self.delayed[queue_name] = still_delayed
            return due_jobs  # Return the list of jobs that were due.

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Return all delayed jobs for this queue.
        """
        async with self.lock:
            out: list[DelayedInfo] = []
            for run_at, job in sorted(self.delayed.get(queue_name, []), key=lambda x: x[0]):
                out.append(DelayedInfo(job_id=job["id"], run_at=run_at, payload=job))
            return out

    async def remove_delayed(self, queue_name: str, job_id: str) -> bool:
        """
        Remove a scheduled-for-later job. Returns True if removed.
        """
        async with self.lock:
            before = len(self.delayed.get(queue_name, []))
            self.delayed[queue_name] = [(ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id]
            return len(self.delayed.get(queue_name, [])) < before

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Return all repeatable-job definitions for this queue.
        """
        async with self.lock:
            out: list[RepeatableInfo] = []
            for rec in self.repeatables.get(queue_name, {}).values():
                out.append(RepeatableInfo(job_def=dict(rec["job_def"]), next_run=rec["next_run"], paused=rec["paused"]))
            return sorted(out, key=lambda x: x.next_run)

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Register or update a backend-managed repeatable definition in memory.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition.

        Returns:
            The next scheduled UNIX timestamp for the definition.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        repeatable_id = repeatable_identity(clean_def)
        next_run = compute_next_run(clean_def)
        async with self.lock:
            self.repeatables.setdefault(queue_name, {})[repeatable_id] = {
                "job_def": clean_def,
                "next_run": next_run,
                "paused": False,
            }
        return next_run

    async def get_due_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Return repeatables that are ready to produce a new job occurrence.

        Args:
            queue_name: The queue to inspect.

        Returns:
            Due repeatable definitions ordered by next run time.
        """
        async with self.lock:
            now = time.time()
            due = [
                RepeatableInfo(job_def=dict(rec["job_def"]), next_run=rec["next_run"], paused=rec["paused"])
                for rec in self.repeatables.get(queue_name, {}).values()
                if not rec["paused"] and rec["next_run"] <= now
            ]
        return sorted(due, key=lambda item: item.next_run)

    async def advance_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Recompute and persist the next run time for a repeatable definition.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition that just fired.

        Returns:
            The new next-run UNIX timestamp.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        repeatable_id = repeatable_identity(clean_def)
        next_run = compute_next_run(clean_def)
        async with self.lock:
            record = self.repeatables.setdefault(queue_name, {}).setdefault(
                repeatable_id,
                {"job_def": clean_def, "paused": False},
            )
            record["job_def"] = clean_def
            record["next_run"] = next_run
            record["paused"] = False
        return next_run

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Remove a repeatable definition.
        """
        async with self.lock:
            repeatable_id = job_def if isinstance(job_def, str) else repeatable_identity(job_def)
            self.repeatables.get(queue_name, {}).pop(repeatable_id, None)

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Mark the given repeatable paused so scheduler skips it.
        """
        async with self.lock:
            repeatable_id = repeatable_identity(job_def)
            if repeatable_id in self.repeatables.get(queue_name, {}):
                self.repeatables[queue_name][repeatable_id]["paused"] = True

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Un-pause the repeatable and return its newly computed next_run timestamp.
        """
        async with self.lock:
            repeatable_id = repeatable_identity(job_def)
            rec = self.repeatables.get(queue_name, {}).get(repeatable_id)
            if rec is None:
                raise KeyError(f"Repeatable job not found: {job_def}")
            clean_def = normalize_repeatable_job_def(rec["job_def"])
            next_run = compute_next_run(clean_def)
            self.repeatables.setdefault(queue_name, {})[repeatable_id] = {
                "job_def": clean_def,
                "next_run": next_run,
                "paused": False,
            }
            return next_run

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Cancel a job: remove it from waiting/delayed and mark it cancelled.
        """
        async with self.lock:
            removed = False
            # remove from waiting
            waiting_before = len(self.queues.get(queue_name, []))
            self.queues[queue_name] = [j for j in self.queues.get(queue_name, []) if j.get("id") != job_id]
            removed = removed or len(self.queues.get(queue_name, [])) < waiting_before
            # remove from delayed
            delayed_before = len(self.delayed.get(queue_name, []))
            self.delayed[queue_name] = [(ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id]
            removed = removed or len(self.delayed.get(queue_name, [])) < delayed_before
            # remove from active bookkeeping
            removed = self.active_jobs.pop((queue_name, job_id), None) is not None or removed
            self.heartbeats.pop((queue_name, job_id), None)
            # record cancellation
            self.cancelled.setdefault(queue_name, set()).add(job_id)
            return removed

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Return True if that job has been cancelled.
        """
        async with self.lock:
            return job_id in self.cancelled.get(queue_name, set())

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the processing state of a job in memory.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: A string representing the new state of the job.
        """
        async with self.lock:
            # Update the state for the job key.
            self.job_states[(queue_name, job_id)] = state
            payload = self.job_payloads.get((queue_name, job_id))
            if payload is not None:
                now = time.time()
                payload["status"] = state
                payload["updated_at"] = now
                if state == State.COMPLETED:
                    payload["completed_at"] = now
                elif state in {State.FAILED, State.EXPIRED}:
                    payload["failed_at"] = now

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a completed job in memory.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is to be saved.
            result: The result data of the job.
        """
        async with self.lock:
            # Store the result for the job key.
            self.job_results[(queue_name, job_id)] = result
            payload = self.job_payloads.get((queue_name, job_id))
            if payload is not None:
                payload["result"] = result
                payload["updated_at"] = time.time()

    async def save_job_payload(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Persist an updated canonical payload inside the in-memory backend.

        This updates every in-memory structure that may still hold a copy of
        the payload so queue inspection and future dequeues observe the same
        deduplication metadata and timestamps.

        Args:
            queue_name: The queue that owns the job.
            payload: The full canonical payload to persist.
        """
        job_id = str(payload["id"])
        stored = dict(payload)
        async with self.lock:
            self.job_payloads[(queue_name, job_id)] = stored
            status = stored.get("status")
            if isinstance(status, str):
                self.job_states[(queue_name, job_id)] = status

            for index, job in enumerate(self.queues.get(queue_name, [])):
                if job.get("id") == job_id:
                    self.queues[queue_name][index] = dict(stored)

            delayed_jobs = self.delayed.get(queue_name, [])
            for index, (run_at, job) in enumerate(delayed_jobs):
                if job.get("id") == job_id:
                    delayed_jobs[index] = (run_at, dict(stored))

            dlq_jobs = self.dlqs.get(queue_name, [])
            for index, job in enumerate(dlq_jobs):
                if job.get("id") == job_id:
                    dlq_jobs[index] = dict(stored)

            active = self.active_jobs.get((queue_name, job_id))
            if active is not None:
                self.active_jobs[(queue_name, job_id)] = dict(stored)

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Retrieves the current processing state of a job from memory.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job whose state is requested.

        Returns:
            A string representing the job's state if found, otherwise None.
        """
        # Return the state for the job key if it exists.
        return self.job_states.get((queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Retrieves the result data of a job from memory.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is requested.

        Returns:
            The job's result data if the job is found and has a result,
            otherwise None.
        """
        # Return the result for the job key if it exists.
        return self.job_results.get((queue_name, job_id))

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies in memory.

        Uses dictionaries to track which parent jobs a child job is waiting on
        (`deps_pending`) and which child jobs are waiting on a parent job
        (`deps_children`).

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, expected to contain at least
                      an "id" key and an optional "depends_on" list of parent job IDs.
        """
        child_job_id = job_dict["id"]
        child_pend_key: _JobKey = (queue_name, child_job_id)
        # Get the set of parent job IDs this child depends on.
        parent_deps: set[str] = set(job_dict.get("depends_on", []))

        # If there are dependencies.
        if not parent_deps:
            return  # Exit if no dependencies are specified.

        # Store the set of parent IDs the child is waiting on.
        self.deps_pending[child_pend_key] = parent_deps
        payload = self.job_payloads.get(child_pend_key)
        if payload is not None:
            payload["depends_on"] = sorted(parent_deps)
            payload["updated_at"] = time.time()
        # For each parent ID, register the child as a dependent.
        for parent_id in parent_deps:
            parent_children_key: _JobKey = (queue_name, parent_id)
            # Add the child job ID to the set of children waiting on this parent.
            self.deps_children.setdefault(parent_children_key, set()).add(child_job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        dependent child jobs are now ready to be enqueued.

        Removes the `parent_id` from the pending dependencies set of its children.
        If a child job's pending dependencies set becomes empty, it is enqueued.
        Cleans up dependency tracking data in memory.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        parent_children_key: _JobKey = (queue_name, parent_id)
        # Get a list of child job IDs waiting on this parent.
        children: list[str] = list(self.deps_children.get(parent_children_key, set()))

        # Iterate through each child job ID.
        for child_id in children:
            child_pend_key: _JobKey = (queue_name, child_id)
            # Check if the child job is still in the pending dependencies tracking.
            if child_pend_key in self.deps_pending:
                # Remove the completed parent's ID from the child's pending set.
                self.deps_pending[child_pend_key].discard(parent_id)
                payload = self.job_payloads.get(child_pend_key)
                if payload is not None:
                    if self.deps_pending[child_pend_key]:
                        payload["depends_on"] = sorted(self.deps_pending[child_pend_key])
                    else:
                        payload.pop("depends_on", None)
                    payload["updated_at"] = time.time()
                # Check if the child job now has no pending dependencies.
                if not self.deps_pending[child_pend_key]:
                    # If all dependencies are met, fetch the job data.
                    raw: dict[str, Any] | None = self.job_payloads.get((queue_name, child_id))
                    # If the job data was found.
                    if raw:
                        # Enqueue the child job.
                        await self.enqueue(queue_name, {**raw, "depends_on": []})
                        # Emit an event indicating the job is now ready.
                        await event_emitter.emit("job:ready", {"id": child_id})
                    # Delete the child's empty pending dependencies entry.
                    del self.deps_pending[child_pend_key]
        # Remove the parent's children tracking entry.
        self.deps_children.pop(parent_children_key, None)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously marks the specified queue as paused in memory.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Add the queue name to the set of paused queues.
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously marks the specified queue as resumed in memory.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Remove the queue name from the set of paused queues.
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused in memory.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue is paused, False otherwise.
        """
        # Check if the queue name is in the set of paused queues.
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in memory.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        # Store the progress for the job key.
        self.job_progress[(queue_name, job_id)] = progress
        payload = self.job_payloads.get((queue_name, job_id))
        if payload is not None:
            payload["progress"] = progress
            payload["updated_at"] = time.time()

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation.

        Adds the job payloads to the in-memory list for the queue and updates
        their states to WAITING. The queue is sorted by job priority after adding.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary is expected to contain at least an "id" and
                  optionally a "priority" key.
        """
        async with self.lock:
            # Get or create the list for the queue.
            q = self.queues.setdefault(queue_name, [])
            # Extend the queue list with the new jobs.
            q.extend(jobs)
            # Update the state for each new job to WAITING.
            for job in jobs:
                self.job_states[(queue_name, job["id"])] = State.WAITING
            # Sort the queue based on job priority after adding all jobs.
            q.sort(key=lambda job: job.get("priority", 5))

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Removes jobs from memory based on their state and optional age criteria.

        Iterates through all job states and removes jobs matching the specified
        queue name and state from the state, result, and progress dictionaries.
        Note: Age filtering (`older_than`) is not fully implemented as this
        backend does not store timestamps for all states.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., "completed", "failed").
            older_than: An optional timestamp. This parameter is not fully
                        utilized in this in-memory implementation for all states.
        """
        to_delete: list[_JobKey] = []
        # Iterate through a copy of the job_states items to allow deletion during iteration.
        for (qname, jid), st in list(self.job_states.items()):
            # Check if the job matches the specified queue name and state.
            if qname == queue_name and st == state:
                # Add the job key to the list of keys to delete.
                to_delete.append((qname, jid))

        # Iterate through the list of job keys to delete.
        for key in to_delete:
            # Remove the job's state, result, and progress from the respective dictionaries.
            self.job_states.pop(key, None)
            self.job_results.pop(key, None)
            self.job_progress.pop(key, None)

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits a backend-specific lifecycle event using the global event emitter.

        Args:
            event: The name of the event to emit.
            data: A dictionary containing data associated with the event.
        """
        # Emit the event using the core event emitter.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Creates and returns an anyio Lock instance for synchronization within
        this single process.

        Note: This does not provide a distributed lock across multiple processes.

        Args:
            key: A string key for the lock (not used by anyio.Lock itself).
            ttl: The time-to-live for the lock in seconds (not used by anyio.Lock).

        Returns:
            An anyio.Lock instance.
        """
        # Reuse a keyed lock so concurrent producers/schedulers in this process
        # coordinate on the same critical section.
        if key not in self._locks:
            self._locks[key] = anyio.Lock()
        return self._locks[key]

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs and registers their dependencies
        within this in-memory backend instance.

        This operation is atomic with respect to other operations on this
        backend instance due to the use of the internal lock.

        Args:
            queue_name: The target queue for all jobs in the flow.
            job_dicts: A list of job payloads (dictionaries) to enqueue.
            dependency_links: A list of tuples, where each tuple is
                              (parent_job_id, child_job_id), defining the
                              dependencies within the flow.

        Returns:
            A list of job IDs that were successfully enqueued, in the order
            they were provided in `job_dicts`.
        """
        raise NotImplementedError("atomic_add_flow not supported for InMemoryBackend")

    def _fetch_job_data(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Helper method to find a job's data dictionary in the in-memory queues.

        Note: This method iterates through the queue list and is not efficient
        for large queues. It's primarily used internally for dependency resolution.

        Args:
            queue_name: The name of the queue to search.
            job_id: The ID of the job to find.

        Returns:
            The job data dictionary if found, otherwise None.
        """
        payload = self.job_payloads.get((queue_name, job_id))
        if payload is not None:
            return dict(payload)
        # Iterate through jobs in the waiting queue.
        for job in self.queues.get(queue_name, []):
            # Check if the job ID matches.
            if job.get("id") == job_id:
                return job  # Return the job data if found.
        # Return None if the job was not found in the waiting queue.
        return None

    async def list_queues(self) -> list[str]:
        """
        lists all known queue names in this in-memory backend instance.

        Returns:
            A list of strings, where each string is the name of a queue that
            has had jobs enqueued into it.
        """
        async with self.lock:
            queue_names = set(self.queues.keys())
            queue_names.update(self.delayed.keys())
            queue_names.update(self.dlqs.keys())
            return sorted(queue_names)

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously returns statistics about the number of jobs in different
        states for a specific queue.

        Provides counts for jobs currently waiting, delayed, and in the dead
        letter queue (DLQ).
        """
        async with self.lock:
            waiting = len(self.queues.get(queue_name, []))
            delayed = len(self.delayed.get(queue_name, []))
            failed = len(self.dlqs.get(queue_name, []))

        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        Lists jobs from the in-memory backend filtered by state.

        Args:
            queue_name: The name of the queue to list jobs from.
            state: The job state to filter (waiting, delayed, failed).

        Returns:
            A list of job dictionaries.
        """
        async with self.lock:
            normalized = normalize_job_type(state)

            if normalized == "waiting":
                return [
                    dict(self.job_payloads.get((queue_name, job["id"]), job))
                    for job in self.queues.get(queue_name, [])
                    if matches_job_type(self.job_payloads.get((queue_name, job["id"]), job), normalized)
                ]

            if normalized == "delayed":
                return [
                    dict(self.job_payloads.get((queue_name, job["id"]), job))
                    for _, job in self.delayed.get(queue_name, [])
                    if matches_job_type(self.job_payloads.get((queue_name, job["id"]), job), normalized)
                ]

            if normalized == "failed":
                return [
                    dict(self.job_payloads.get((queue_name, job["id"]), job))
                    for job in self.dlqs.get(queue_name, [])
                    if matches_job_type(self.job_payloads.get((queue_name, job["id"]), job), normalized)
                ]

            return [
                dict(payload)
                for (job_queue, _), payload in self.job_payloads.items()
                if job_queue == queue_name and matches_job_type(payload, normalized)
            ]

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Attempts to retry a job currently in the Dead Letter Queue (DLQ).

        Removes the job from the DLQ and re-enqueues it into the main queue
        with its state updated to WAITING.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to retry.

        Returns:
            True if the job was found in the DLQ and successfully re-enqueued,
            False otherwise.
        """
        async with self.lock:
            dlq = self.dlqs.get(queue_name, [])
            # Iterate through the DLQ list.
            for job in dlq:
                # Check if the job ID matches.
                if job["id"] == job_id:
                    # Remove the job from the DLQ.
                    dlq.remove(job)
                    # Add the job back to the main queue.
                    self.queues.setdefault(queue_name, []).append(job)
                    # Update the job's state to WAITING.
                    self.job_states[(queue_name, job_id)] = State.WAITING
                    self.job_payloads[(queue_name, job_id)] = {
                        **job,
                        "status": State.WAITING,
                        "updated_at": time.time(),
                    }
                    return True  # Indicate successful retry.
            return False  # Indicate job was not found in DLQ.

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Removes a job from any of the in-memory queues (waiting, delayed, or DLQ).

        Also removes the job's state, result, and progress information.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if the job was found and removed, False otherwise.
        """
        async with self.lock:
            removed = False

            waiting = self.queues.get(queue_name, [])
            orig_wait = len(waiting)
            self.queues[queue_name] = [j for j in waiting if j.get("id") != job_id]
            if len(self.queues[queue_name]) != orig_wait:
                removed = True

            delayed = self.delayed.get(queue_name, [])
            orig_delayed = len(delayed)
            self.delayed[queue_name] = [(ts, j) for ts, j in delayed if j.get("id") != job_id]
            if len(self.delayed[queue_name]) != orig_delayed:
                removed = True

            dlq = self.dlqs.get(queue_name, [])
            orig_dlq = len(dlq)
            self.dlqs[queue_name] = [j for j in dlq if j.get("id") != job_id]
            if len(self.dlqs[queue_name]) != orig_dlq:
                removed = True

            if removed:
                key = (queue_name, job_id)
                self.job_states.pop(key, None)
                self.job_results.pop(key, None)
                self.job_progress.pop(key, None)
                self.active_jobs.pop(key, None)
                self.heartbeats.pop(key, None)
                self.job_payloads.pop(key, None)
                return True

            return False

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Records or updates the last heartbeat timestamp for a specific job in memory.

        This method acquires an exclusive lock to safely update the in-memory
        heartbeats dictionary, associating the provided timestamp with the given
        job identified by its queue name and job ID.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp (float) representing the time of the heartbeat.
        """
        # Acquire the lock to ensure safe concurrent modification of self.heartbeats
        async with self.lock:
            # Store the heartbeat timestamp. The key is a tuple combining
            # queue name and job ID for uniqueness.
            self.heartbeats[(queue_name, job_id)] = timestamp
            payload = self.job_payloads.get((queue_name, job_id))
            if payload is not None:
                payload["heartbeat"] = timestamp
                payload["updated_at"] = time.time()

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Retrieves a list of jobs currently tracked by heartbeats that have not
        sent a heartbeat since a specified timestamp.

        This method iterates through all recorded heartbeats, checks if a heartbeat
        timestamp is older than the `older_than` threshold, and if so, attempts
        to find the corresponding job's full data payload within the in-memory queues.

        Args:
            older_than: A Unix timestamp (float). Any job with a heartbeat timestamp
                        strictly less than this value will be considered stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' and
            'job_data' for each stalled job found. 'job_data' is the original
            dictionary payload of the job from the queues.
        """
        stalled: list[dict[str, Any]] = []  # Initialize an empty list to collect stalled job details

        # Acquire the lock to ensure safe concurrent access to both self.heartbeats
        # and self.queues during the check and lookup process.
        async with self.lock:
            # Iterate over a copy of the heartbeats items. This avoids potential issues
            # if heartbeats were modified by another task during iteration (though
            # modifications are limited by the lock here, copying is a robust pattern).
            for (q, jid), ts in list(self.heartbeats.items()):
                # Check if the job's last heartbeat timestamp is older than the threshold
                if ts < older_than:
                    payload = self.active_jobs.get((q, jid))
                    if payload is None:
                        payload = self._fetch_job_data(q, jid)
                    # If a corresponding job payload was found in the queue, add it
                    # to the list of stalled jobs with its queue name.
                    if payload:
                        stalled.append({"queue_name": q, "job_data": payload})

        return stalled  # Return the list of identified stalled jobs

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Re-enqueues a stalled job back onto its waiting queue in memory and removes
        its associated heartbeat entry.

        This method assumes the job is being moved from a "running" or "stalled"
        conceptual state back to a "waiting" state. It appends the job's data
        back to the end of the specified queue in the in-memory queues dictionary
        and removes the job's entry from the in-memory heartbeats dictionary,
        signifying it's no longer actively being tracked by heartbeat.

        Args:
            queue_name: The name of the queue the job should be added back into.
            job_data: The dictionary containing the job's data payload. This
                      dictionary must contain an 'id' key to identify the job
                      for heartbeat removal.
        """
        # Acquire the lock to ensure safe concurrent modification of both self.queues
        # and self.heartbeats.
        async with self.lock:
            # Append the job data back to the list associated with the queue_name.
            # setdefault(queue_name, []) ensures that if the queue_name does not
            # already exist as a key in self.queues, it is created with an empty
            # list as its value before appending the job_data.
            self.queues.setdefault(queue_name, []).append(job_data)
            self.job_states[(queue_name, job_data["id"])] = State.WAITING
            self.job_payloads[(queue_name, job_data["id"])] = {
                **job_data,
                "status": State.WAITING,
                "updated_at": time.time(),
            }

            # Remove the heartbeat entry for this specific job from the heartbeats
            # dictionary. This stops tracking its heartbeat as it's now re-enqueued.
            # Use .pop((queue_name, job_data["id"]), None) with a default of None
            # to prevent a KeyError if the heartbeat entry was somehow already removed
            # (e.g., by another process or cleanup task, or if 'id' is missing).
            # Note: Accessing job_data["id"] assumes 'id' key exists; a safer way
            # might be job_data.get("id").
            self.heartbeats.pop((queue_name, job_data["id"]), None)
            self.active_jobs.pop((queue_name, job_data["id"]), None)

    async def register_worker(
        self,
        worker_id: str,
        queue: str,
        concurrency: int,
        timestamp: float,
    ) -> None:
        """
        Register or update a worker's heartbeat in the in-memory registry.

        Stores or updates the WorkerInfo for a specific worker in the
        internal dictionary `_worker_registry`.

        Args:
            worker_id: The unique identifier for the worker.
            queue: The name of the queue the worker is associated with.
            concurrency: The concurrency level of the worker.
            timestamp: The timestamp representing the worker's last heartbeat.
        """
        self._worker_registry[worker_id] = WorkerInfo(
            id=worker_id,
            queue=queue,
            concurrency=concurrency,
            heartbeat=timestamp,
        )

    async def deregister_worker(self, worker_id: str) -> None:
        """
        Remove a worker's entry from the in-memory registry.

        Removes the entry for the specified worker_id from the
        internal dictionary `_worker_registry`.

        Args:
            worker_id: The unique identifier of the worker to deregister.
        """
        self._worker_registry.pop(worker_id, None)

    async def list_workers(self) -> list[WorkerInfo]:
        """
        Lists active workers from the in-memory registry.

        Iterates through the workers in the internal dictionary `_worker_registry`
        and returns a list of those whose last heartbeat is within the
        configured time-to-live (TTL).

        Returns:
            A list of WorkerInfo objects representing the active workers.
        """
        from asyncmq import monkay

        now = time.time()
        return [
            info for info in self._worker_registry.values() if now - info.heartbeat <= monkay.settings.heartbeat_ttl
        ]
