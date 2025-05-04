from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.contrib.dashboard.engine import templates

# Dummy data for jobs in a queue
dummy_jobs = {
    "default": [
        {"id": "job1", "status": "waiting", "created_at": "2024-05-04 10:00", "args": ["arg1"], "result": None},
        {"id": "job2", "status": "active", "created_at": "2024-05-04 10:01", "args": ["arg2"], "result": None},
        {"id": "job3", "status": "completed", "created_at": "2024-05-04 10:02", "args": ["arg3"], "result": "ok"},
    ]
}

async def queue_jobs(request: Request) -> HTMLResponse:
    queue_name = request.path_params.get("name", "default")
    jobs = dummy_jobs.get(queue_name, [])
    return templates.get_template_response(request, "jobs.html", {
        "request": request,
        "title": f"Jobs in '{queue_name}'",
        "queue": queue_name,
        "jobs": jobs
    })
