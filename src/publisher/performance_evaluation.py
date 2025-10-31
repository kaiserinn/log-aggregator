import httpx
import os
import asyncio
from time import time
from src.publisher.publish import generate_events

HOST = os.getenv("AGG_HOST", "localhost")
PORT = os.getenv("AGG_PORT", "7878")
URL = f"http://{HOST}:{PORT}/publish"

n_counts = [10, 100, 1000, 10000]

async def main():
    single_elapsed = []
    batched_elapsed = []

    async with httpx.AsyncClient() as client:
        for n in n_counts:
            start = time()
            events = generate_events(n, 0.2)
            for event in events:
                await client.post(URL, json=event)
            elapsed = time() - start
            single_elapsed.append(elapsed)
        for n in n_counts:
            start = time()
            await client.post(URL, json=generate_events(n, 0.2))
            elapsed = time() - start
            batched_elapsed.append(elapsed)

    header = f"{'N Events':<10} | {'Avg Single Latency':<20} | {'Batch Latency':<18} | {'Single Throughput':<22} | {'Batched Throughput':<22}"
    print(header)
    print("-" * len(header))

    for i, n in enumerate(n_counts):
        # **FIX**: Use new variables (s_time, b_time)
        # Your original code had a bug here that would crash the loop
        s_time = single_elapsed[i]
        b_time = batched_elapsed[i]

        s_avg_latency = (s_time / n) if n > 0 else 0
        b_total_latency = b_time

        s_throughput = (n / s_time) if s_time > 0 else float('inf')
        b_throughput = (n / b_time) if b_time > 0 else float('inf')

        # --- THIS IS THE CORRECTED LINE ---
        # We create the string with units *first*, then pad it to the header width
        print(
            f"{n:<10} | "
            f"{f'{s_avg_latency:.6f}s':<20} | "
            f"{f'{b_total_latency:.6f}s':<18} | "
            f"{f'{s_throughput:.2f} events/s':<22} | "
            f"{f'{b_throughput:.2f} events/s':<22}"
        )

if __name__ == '__main__':
    asyncio.run(main())

