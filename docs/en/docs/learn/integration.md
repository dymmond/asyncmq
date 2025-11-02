# Esmerald Integration Tutorial

Welcome to a hands-on, behind-the-scenes guide for integrating **AsyncMQ** into your **Esmerald** application.
We'll sprinkle in professional insights and a dash of humor, because who said documentation has to be dry?

We will be running examples using [Esmerald](https://esmerald.dev) because AsyncMQ is from the same authors so it makes
sense to use the tools of the ecosystem but this is not **mandatory** as you can replace Esmerald with any other
ASGI framework such as FastAPI, Sanic, Quartz... You name it :rocket:.

---

By the end of this tutorial, you'll know how to:

* Configure AsyncMQ in your Esmerald app with custom settings
* Define and register tasks that run in the background
* Enqueue jobs from HTTP endpoints (no blocking your main thread!)
* Launch and manage workers via ASGI lifespan events
* Gracefully shut down workers and check health
* Handle errors, retries, and dead-letter queues like a pro
* Avoid the most common pitfalls—and laugh about them later

---

## 1. Project Setup

First things first: let's get your project scaffolded and dependencies squared away.

```bash
mkdir asyncmq-esmerald-example
cd asyncmq-esmerald-example
poetry init -n                     # 🦄 Instant project 🛠️
poetry add esmerald uvicorn asyncmq redis anyio
```

!!! Check
    **Why Poetry?** Because pinning dependencies is like using seat belts you will thank yourself later when nothing
    breaks unexpectedly. 🤓

    Also, we will be using `poetry` because it is widely adopted by the community but you can change it to whatever like `uv`,
    `hatch` (which is what AsyncMQ uses for development and deployment), `pdm`...

Your directory should look like:

```
asyncmq_esmerald/
├── tasks.py       # 🗂️ Defines your background jobs
├── settings.py    # ⚙️ AsyncMQ & Redis configuration
└── app.py         # 🚀 Esmerald application
```

### 1.1. The custom `settings.py`

Centralize your AsyncMQ settings so you don't lose sleep over environment drift.

```python
{!> ../../../docs_src/tutorial/settings.py !}
```

Load your custom settings before anything else runs:

```bash
export ASYNCMQ_SETTINGS_MODULE=asyncmq_esmerald.settings.Settings
```

This is a pattern that all tools of [Dymmond](https://dymmond.com) use and this allows flexibility and extendability
introducing the separation of concerns and environments without polluting your codebase.

This allows you to simply isolate each settings by its corresponding responsabilities.

!!! Tip
    **Pro tip:** Export this in your shell's startup file (e.g., `~/.bashrc`) or `.env` file.

---

## 2. Defining Tasks (`tasks.py`)

Tasks are your building blocks—think of them as mini-applications that run outside the request/response cycle.

```python
{!> ../../../docs_src/tutorial/tasks.py !}
```

### Why these parameters?

* **queue**: logically groups tasks; you can dedicate separate queues to different workloads (e.g., `reports`, `images`).
* **retries**: automatically retry transient failures—network hiccups, API rate limits—without manual intervention.
* **ttl**: cap the lifetime of a stuck job; after `ttl` seconds, it goes to the Dead-Letter Queue (DLQ) to avoid clutter.

!!! Warning
    Avoid CPU-bound operations here (e.g., large data crunching), they block threads. Offload heavy lifting to
    specialized services or use `anyio.to_thread` consciously.

---

## 3. Enqueuing via Esmerald Gateway (`app.py`)

Your Esmerald endpoint becomes the order desk for background work: submit a request, get an immediate response,
and let AsyncMQ handle the prep.

```python
{!> ../../../docs_src/tutorial/app.py !}
```

### What just happened?

1. **Signup Endpoint**: Accepts a JSON payload, calls `send_welcome.enqueue(...)`, and returns immediately with a `job_id`.
2. **Health Endpoint**: Uses `queue_stats()` to expose counts of `waiting`, `active`, `completed`, and `failed` jobs,
ideal for monitoring dashboards.
3. **Lifespan Hooks**: Leverage Esmerald's ASGI lifespan to spin up `email_queue.run()` right after startup and shut it down
cleanly on server stop.

!!! Check
    **Why `queue.run()` instead of `start()`?** `run()` exposes granular control—handles delayed scanners,
    repeatable jobs, and rate limiting exactly as configured.

---

## 4. Graceful Shutdown & Health Checks

A robust app handles traffic spikes, failures, and deployments without dropping work.

### 4.1. Graceful Shutdown

* **Cancellation**: We cancel the worker task, which triggers cleanup in `run_worker`.
* **In-flight Jobs**: Worker waits for currently processing jobs to finish or hit a retry count before exiting.
* **Avoid Data Loss**: Unacknowledged jobs stay in the queue; they'll be picked up by the next worker.

### 4.2. Health Checks & Metrics

* Expose `queue_stats()` for Prometheus scraping or uptime monitors.
* Hook into `event_emitter` for granular metrics:

    ```python
    {!> ../../../docs_src/tutorial/emitter.py !}
    ```

* Gauge queue length, processing time, failure rates, know your bottlenecks!

---

## 5. Error Handling & Retries

1. **Retries**: If `send_welcome` throws an exception, AsyncMQ will retry it up to `retries` times, honored in FIFO with backoff (if configured).
2. **Dead-Letter Queue**: After exhausting retries, jobs land in `email:dlq`—inspect with:

   ```bash
   asyncmq job list --queue email --state failed
   ```
3. **Manual Replay**: Resurrect failed jobs once you've fixed the root cause:

   ```bash
   asyncmq job retry <failed_job_id> --queue email
   ```

!!! Check
    **Humorous moment:** Treat your DLQ like voicemail, don't ignore it forever, or you'll miss urgent messages! 📬

---

## 6. Best Practices & Pitfalls

* **Use `anyio.to_thread` for CPU-bound tasks** to avoid clogging worker threads.
* **Pin versions** of AsyncMQ and Redis for reproducibility.
* **Monitor Redis**: Watch out for key bloat if you schedule tons of delayed jobs.
* **Tune `scan_interval`**: Lower for low-latency needs; higher for reducing Redis polling cost.
* **Centralize settings**: Keep `ASYNCMQ_SETTINGS_MODULE` consistent across environments to avoid "it works on my machine" syndrome.
* **Leverage Events**: Integrate with observability stacks (Prometheus, Sentry) using `event_emitter` hooks.

---

Congratulations, you've mastered AsyncMQ in Esmerald!
In the next chapter, we'll explore **Advanced Patterns** like custom backends, DAG orchestration, and Kubernetes scaling.
