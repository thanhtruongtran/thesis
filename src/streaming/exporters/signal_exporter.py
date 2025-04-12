import time
import json
from src.databases.blockchain_etl import BlockchainETL
from src.models.loader import Loader
from src.services.queue.events_stream import SignalsStream
from src.constants.time import TimeConstants
from src.utils.time import round_timestamp


class SignalExporter:
    def __init__(self, signal_stream: SignalsStream, db: BlockchainETL = None):
        self._db = db
        self._signal_stream = signal_stream

    def export_items(self, items):
        signals = []
        for item in items:
            signals.append(item)
            self._signal_stream.publish_signal(item)

        # if signals and self._db:
        #     self._db.update_signals(signals)

        if self._signal_stream:
            self._count_signals_to_redis(signals)

    def _count_signals_to_redis(self, items):
        timestamp = round_timestamp(int(time.time()), TimeConstants.MINUTES_15)

        redis_key = f"signal_count:{timestamp}"
        count = len(items)

        self._signal_stream.incrby(redis_key, count, TimeConstants.A_DAY*2)

    def get_whales_wallet(self, chain_id):
        cache_key = f"whale_wallets_{chain_id}"
        redis_client = self._signal_stream._redis
        data = redis_client.get(cache_key)
        if data:
            return json.loads(data)
        
        return None

    def cache_whales_wallet(self, items, chain_id):
        if not items:
            return

        cache_key = f"whale_wallets_{chain_id}"
        redis_client =self._signal_stream._redis
        redis_client.setex(cache_key, 86400, json.dumps(items))

    def get_loader(self, key: str) -> Loader:
        if not self._db:
            return Loader(_id=key)
        
        data = self._db.get_config(key)
        if not data:
            return Loader(_id=key)

        loader = Loader()
        loader.from_dict(data)
        return loader

    def update_loader(self, loader: Loader) -> None:
        data = loader.to_dict()
        data['_id'] = loader.id
        if not self._db:
            return
        self._db.update_config(data)
