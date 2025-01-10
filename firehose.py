from types import FrameType
from typing import Any
import collections
import time
import json

from atproto import CAR, AtUri, FirehoseSubscribeReposClient, firehose_models, models, parse_subscribe_repos_message

counters = collections.Counter()
timer_start = time.time()
file_handle = None
curr_file_num = 0
curr_records_in_file = 0
MAX_RECORDS_PER_FILE = 100000

def get_new_file():
    global file_handle, curr_file_num, curr_records_in_file

    print('Reached max records. Creating new file...')

    if file_handle:
        file_handle.close()
    
    curr_file_num += 1 
    filename = f'post_data_{curr_file_num}.log'
    file_handle = open(filename, "a")
    curr_records_in_file = 0


def increment_counter(counter_name: str) -> None:
    counters[counter_name] += 1
    if counters[counter_name] % 500 == 0:
        print(f'{counter_name}: {counters[counter_name]} ({counters[counter_name] / (time.time() - timer_start):.2f} per second)')
        

def on_message_handler(message: firehose_models.MessageFrame) -> None:
    global curr_records_in_file

    if not file_handle:
        get_new_file()

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
            curr_records_in_file += 1

            if curr_records_in_file >= MAX_RECORDS_PER_FILE:
                get_new_file()

                
client = FirehoseSubscribeReposClient()
try:
    client.start(on_message_handler)
finally:
    if file_handle:
        file_handle.close()