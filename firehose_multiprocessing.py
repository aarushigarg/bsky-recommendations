# using https://github.com/MarshalX/atproto/blob/main/examples/firehose/process_commits.py

import multiprocessing
import signal
import time
from collections import defaultdict
from types import FrameType
from typing import Any
import json

from atproto import CAR, AtUri, FirehoseSubscribeReposClient, firehose_models, models, parse_subscribe_repos_message

TOTAL_RECORDS = multiprocessing.Value('i', 0)
MAX_RECORDS = 10

FILE_HANDLE = open('post_data.log', 'a')

def worker_main(cursor_value: multiprocessing.Value, pool_queue: multiprocessing.Queue) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # we handle it in the main process

    while True:
        message = pool_queue.get()

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            continue

        if commit.seq % 20 == 0:
            cursor_value.value = commit.seq

        if not commit.blocks:
            continue

        car = CAR.from_bytes(commit.blocks)
        for op in commit.ops:
            if op.action != 'create':
                continue

            uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

            if not op.cid:
                continue

            record_raw_data = car.blocks.get(op.cid)
            print(f"Writing record to file: {record_raw_data}")
            if not record_raw_data or record_raw_data.get('$type') != 'app.bsky.feed.post':
                continue

            record_raw_data.update({'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo})

            try:
                # Write to log file
                FILE_HANDLE.write(json.dumps(record_raw_data))
                FILE_HANDLE.write('\n')
                FILE_HANDLE.flush()

                # Increment counter
                with TOTAL_RECORDS.get_lock():
                    TOTAL_RECORDS.value += 1
                    if TOTAL_RECORDS.value >= MAX_RECORDS:
                        print(f"Reached {MAX_RECORDS} records. Shutting down...")
                        pool_queue.task_done()
                        return

            except TypeError as e:
                # Skip records that can't be serialized due to bytes objects
                continue

def get_firehose_params(cursor_value: multiprocessing.Value) -> models.ComAtprotoSyncSubscribeRepos.Params:
    return models.ComAtprotoSyncSubscribeRepos.Params(cursor=cursor_value.value)

def measure_events_added_per_second(func: callable) -> callable:
    def wrapper(*args) -> Any:
        wrapper.calls += 1
        if wrapper.calls % 1000 != 0:
            return
        
        cur_time = time.time()

        if cur_time - wrapper.start_time >= 1:
            print(f'NETWORK LOAD: {int(wrapper.calls/(cur_time - wrapper.start_time))} events/second')
            wrapper.start_time = cur_time
            wrapper.calls = 0

        return func(*args)

    wrapper.calls = 0
    wrapper.start_time = time.time()

    return wrapper

def signal_handler(_: int, __: FrameType) -> None:
    print('Keyboard interrupt received. Waiting for the queue to empty before terminating processes...')

    # Stop receiving new messages
    client.stop()

    # Drain the messages queue
    while not queue.empty():
        print(f'Waiting for the queue to empty...')
        time.sleep(1)

    print('Queue is empty. Gracefully terminating processes...')

    pool.terminate()
    pool.join()

    exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    start_cursor = None

    params = None
    cursor = multiprocessing.Value('i', 0)
    if start_cursor is not None:
        cursor = multiprocessing.Value('i', start_cursor)
        params = get_firehose_params(cursor)

    client = FirehoseSubscribeReposClient(params)

    workers_count = multiprocessing.cpu_count() * 2 - 1
    max_queue_size = 10000

    queue = multiprocessing.Queue(maxsize=max_queue_size)
    pool = multiprocessing.Pool(workers_count, worker_main, (cursor, queue))

    @measure_events_added_per_second
    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        if cursor.value:
            client.update_params(get_firehose_params(cursor))
        
        # Check if we've reached the record limit
        with TOTAL_RECORDS.get_lock():
            if TOTAL_RECORDS.value >= MAX_RECORDS:
                print("Maximum records reached. Stopping client...")
                client.stop()
                pool.terminate()
                pool.join()
                exit(0)

        queue.put(message)

    client.start(on_message_handler)