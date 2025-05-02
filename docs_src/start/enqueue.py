# app.py (continued)
import anyio


async def main():
    # This schedules your function to run on a worker, it does NOT execute it immediately:
    await say_hello.enqueue(
        "World"
    )
    print("Job enqueued to 'default' queue.")

if __name__ == "__main__":
    anyio.run(main)
