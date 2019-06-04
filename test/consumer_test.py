import unittest
import os
import uuid
os.environ['SHARD_NUMBER'] = '0'
import consumer


class UnitTest(unittest.TestCase):
    def event_time_to_s3_key_test(self):
        event_time = '2019-05-28 12:25:03.000'
        file_name = str(uuid.uuid4())
        s3_file_name = "y=2019/m=5/d=28/h=12/f1d20c41-d7c4-4e71-a2b0-c8fa6f00cff3.json"
        assert consumer.event_time_to_s3_key(event_time, file_name) in s3_file_name
