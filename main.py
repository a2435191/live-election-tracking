import asyncio
import json
import math
import os.path
import random
from collections import defaultdict
from datetime import datetime, timedelta
from random import randrange
from typing import NamedTuple
import pandas as pd
import os
import aiohttp


async def collect_data(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if 400 <= resp.status < 500:
                print("failed:", url, resp.status)
            try:
                return json.loads(await resp.text())
            except json.JSONDecodeError:
                print(resp)


async def main():
    START = 22615
    END   = 22807
    
    APPROX_WAIT_SECS = 15.00

    prev: dict[int, dict] = {}
    num_loops = 0

    while True:
        try:
            main_tasks: list[asyncio.Task] = []
            # google_sheets_tasks: list[asyncio.Task] = []
            for race_id in range(START, END + 1):
                url = f"https://data.ddhq.io/{str(race_id).zfill(5)}"
                request_task = asyncio.create_task(collect_data(url), name=race_id)

                def _on_complete(task: asyncio.Task) -> asyncio.Task | None:
                    result: dict = task.result()

                    if result is None:
                        print("result is none for", task.get_name())
                        return

                    race_id = str(result.get("race_id", "Unknown Race ID"))
                    state = result.get('state', 'Unknown State')
                    office = str(result.get('office', 'Unknown Office')).replace('/', '__BACKSLASH__')

                    save_dir = f"outputs/april-4/{race_id}:{state}:{office}"

                    timestamp = datetime.now()

                    if result == prev.get(race_id):
                        pass
                        #print(f"no change for {url} at {timestamp}")
                    else:
                        if not os.path.exists(save_dir):
                            os.mkdir(save_dir)

                        save_path = os.path.join(
                            save_dir, f"{timestamp}.json")
                        print("saving to", save_path)
                        with open(save_path, 'w+') as fh:
                            json.dump(result, fh, indent=2)

                        # if url == 'https://data.ddhq.io/22547':
                        #     google_sheets_task = asyncio.create_task(
                        #         asyncio.to_thread(
                        #             upload_to_google_sheets, url, result, timestamp)
                        #     )
                        #     google_sheets_tasks.append(google_sheets_task)

                    prev[race_id] = result

                request_task.add_done_callback(_on_complete)
                # main_tasks.append(request_task)

            sleep_coro = asyncio.sleep(APPROX_WAIT_SECS)
            await asyncio.gather(*main_tasks)

            # all_sheets_task = asyncio.gather(*google_sheets_tasks)
            await sleep_coro
            # await all_sheets_task

            num_loops += 1

        except Exception as e:
            os.system(
                f"osascript -e 'display notification \"{e}\" with title \"Exception thrown\"'")
            print(e)
            while True:
                input("press enter to continue: ")
                break


if __name__ == '__main__':
    asyncio.run(main())
        
