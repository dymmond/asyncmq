# Quickstart

Get started with AsyncMQ in just a few steps: register a task, enqueue a job, and launch a worker.

## 1. Register a Task

Apply the `@task` decorator to an async function, which does **not** automatically enqueue jobs on invocation—instead, it:

1. Wraps your function so workers know how to execute it.
2. Adds an `enqueue` helper method to schedule jobs.
3. Stores task metadata (queue, retries, TTL, etc.) in `TASK_REGISTRY`.

##  Create your backend (Redis as default)

```python
{!> ../docs_src/start/quickstart.py !}
```

!!! Warning
    Calling `say_hello("World")` executes the function immediately, without enqueuing.
    To schedule a background job, you must use `say_hello.enqueue(...)`.

## 2. Enqueue a Job (delay)

Use the generated `enqueue` helper on your task. All optional parameters have sensible defaults:

* `delay=0`
* `priority=5`
* `depends_on=None`
* `repeat_every=None`
* `backend` is optional, you can pass here the instance or it will load the default from the settings.

```python
{!> ../docs_src/start/enqueue.py !}
```

Run it:

```bash
python app.py
```

Enqueue a Job

Use the generated `enqueue` helper on your task. All optional parameters have sensible defaults:

* `delay=0`
* `priority=5`
* `depends_on=None`
* `repeat_every=None`

```python
# app.py (continued)
async def main():
    # Enqueue a job; this helper returns None
    await say_hello.enqueue(backend, "World")
    print("Job enqueued to 'default' queue.")

if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```bash
python app.py
```

## 3. Inspect Jobs with the CLI

Peek at pending and completed jobs:

* List waiting (pending) jobs `asyncmq job list --queue default --state waiting`

```bash
asyncmq job list --queue default --state waiting
```

* List completed jobs `asyncmq job list --queue default --state completed`

```bash
asyncmq job list --queue default --state completed
```

## 4. Launch a Worker

Start a worker to process tasks from the `default` queue:

```bash
# Default concurrency (1) and backend (from settings or Redis)
asyncmq worker start default

# Override concurrency
asyncmq worker start default --concurrency 2
```

You should see output like:

```text
[INFO] Starting worker for queue 'default' with concurrency=1
```

---

Congratulations, your first AsyncMQ task is live!

Next: **Core Concepts & Architecture** to explore queues, jobs, workers, and storage backends in depth.
