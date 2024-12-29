from types import FrameType
from typing import Any
import collections
import time
import json

from atproto import CAR, AtUri, FirehoseSubscribeReposClient, firehose_models, models, parse_subscribe_repos_message

counters = collections.Counter()
timer_start = time.time()
file_handle = open('post_data.log', 'a')

MAX_RECORDS = 10000

def increment_counter(counter_name: str) -> None:
    counters[counter_name] += 1
    if counters[counter_name] % 500 == 0:
        print(f'{counter_name}: {counters[counter_name]} ({counters[counter_name] / (time.time() - timer_start):.2f} per second)')
        

def on_message_handler(message: firehose_models.MessageFrame) -> None:
    increment_counter('messages_received')

    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit) or not commit.blocks:
        increment_counter('invalid_commits')
        return

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action != 'create' or not op.cid:
            increment_counter('invalid_ops')
            continue

        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')
        record_raw_data = car.blocks.get(op.cid)
        record_raw_data.update({'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo})
        
        increment_counter(f'processed_records_{record_raw_data.get("$type")}')

        if record_raw_data.get('$type') == 'app.bsky.feed.post':
            file_handle.write(json.dumps(record_raw_data, default=repr))
            file_handle.write('\n')
            file_handle.flush()

            increment_counter('records_written')
            if counters['records_written'] >= MAX_RECORDS:
                print('Reached max records. Shutting down...')
                client.stop()

client = FirehoseSubscribeReposClient()
client.start(on_message_handler)