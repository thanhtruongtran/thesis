import time
import json
import redis
from random import random

from src.utils.logger import get_logger
from src.constants.config import RedisConfig

logger = get_logger('SignalsStream')


class SignalsStream:
    def __init__(self, r):
        self._redis: redis.Redis = r

        self.stream_name = "signal_stream"
        self.group_name = "signal_group"
        self.consumer_name = "signal_consumer"

        self.max_len = 100

        self._create_consumer_group()

    def _create_consumer_group(self):
        try:
            self._redis.xgroup_create(self.stream_name, self.group_name, id="$", mkstream=True)
            logger.info(f"Consumer group '{self.group_name}' created.")
        except redis.exceptions.ResponseError:
            logger.info(f"Consumer group '{self.group_name}' already exists.")

    def publish_signal(self, signal):
        message = {'msg': json.dumps(signal)}
        self._redis.xadd(self.stream_name, message, maxlen=self.max_len)

    def subscribe_signal(self, timeout=300):
        start_time = time.time()
        while True:
            messages = self._redis.xreadgroup(self.group_name, self.consumer_name, {self.stream_name: ">"}, count=1, block=1000)
            for s, entries in messages:
                for entry_id, data in entries:
                    yield data

                    self._redis.xack(self.stream_name, self.group_name, entry_id)

            if time.time() - start_time > timeout:
                break

    def incrby(self, key, count, seconds):
        try:
            x = self._redis.incrby(key, count)
            if x == count: 
                self._redis.expire(key, seconds)
        except Exception as e:
            logger.error(f"[Redis] incrby failed: {e}")


def simulate_stream():
    stream = SignalsStream(redis.from_url(RedisConfig.CONNECTION_URL, decode_responses=True))
    cnt = 0
    while True:
        cnt += 1
        stream.publish_signal({'name': 'test', 'count': cnt})
        logger.info(f"Published signal {cnt}")
        time.sleep(10 * random())


def check_signal_latency(signal_stream: SignalsStream, count=10):
    messages = signal_stream._redis.xrevrange(signal_stream.stream_name, count=count)

    for entry_id, data in messages:
        try:
            redis_ts_ms = int(entry_id.split('-')[0])/1000
            signal = json.loads(data.get("msg", "{}"))
            block_ts = int(signal.get("timestamp", 0))

            if block_ts:
                delay_sec = round((redis_ts_ms - block_ts), 2)
                signal_id = signal.get("id", "unknown")

                print(f"[{signal_id}] Delay: {delay_sec}s ")

        except Exception as e:
            logger.error(f"Failed to parse signal: {e}")


if __name__ == '__main__':
    stream = SignalsStream(redis.from_url(RedisConfig.CONNECTION_URL, decode_responses=True))
    delays = check_signal_latency(stream, count=10)