from asyncmq.tasks import task


@task(queue="reports", progress=True)
async def generate_report(data: list[int], report_id: str, report_progress):
    total = len(data)
    for i, chunk in enumerate(data, start=1):
        # process chunk...
        report_progress(i/total, {"current": i})
    return "done"
