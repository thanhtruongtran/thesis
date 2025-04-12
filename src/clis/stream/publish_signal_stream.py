import click
import redis

from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_dex import MongoDBDex
from src.databases.mongodb_sc_label import MongoDBSCLabel
from src.services.queue.events_stream import SignalsStream
from src.streaming.adapters.publish_signal_adapter import PublishSignalAdapter
from src.streaming.exporters.signal_exporter import SignalExporter
from src.streaming.streamer import Streamer
from src.constants.blockchain_etl import DBPrefix
from src.constants.network import Chains
from src.utils.logger import get_logger
from src.constants.config import RedisConfig

logger = get_logger('Publish Signal Stream')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-block-file', default='last_synced_block.txt', show_default=True, type=str, help='')
@click.option('--lag', default=0, show_default=True, type=int, help='The number of blocks to lag behind the network.')
@click.option('-s', '--start-block', default=None, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', type=int, default=None, show_default=True, help='End block')
@click.option('-b', '--batch-size', default=4, show_default=True, type=int,
              help='How many blocks to batch in single request')
@click.option('-w', '--max-workers', default=8, show_default=True, type=int, help='The number of workers')
@click.option('-B', '--block-batch-size', default=320, show_default=True, type=int,
              help='How many blocks to batch in single sync round')
@click.option('-c', '--chain-id', default='0x138de', show_default=True, type=str, help='Chain id')
@click.option('--query-batch-size', default=2000, show_default=True, type=int,
              help='How many query to batch in single request')
@click.option('--pid-file', default=None, show_default=True, type=str, help='pid file')
@click.option('--forks', default=None, show_default=True, type=str, help='Forks')
@click.option('--stream-id', default=None, show_default=True, type=str, help='streamer id')
@click.option('--collector-id', default=["liquidity-pool-events-collector", "lending_events"], show_default=True, type=str, multiple=True, help='collector id')
def publish_signal_stream(
        last_synced_block_file, lag, start_block, end_block, batch_size, max_workers,
        block_batch_size, query_batch_size, pid_file, chain_id, forks=None, collector_id="", stream_id=None         
):
    """Streaming alert dex large add liquidity. """
    db_prefix = DBPrefix.mapping[Chains.names[chain_id]]
    _importer = BlockchainETL(db_prefix=db_prefix)
    logger.info(f'Connect to importer: {_importer.connection_url}')

    _dex_db = MongoDBDex()
    _klg_db = MongoDBKLG()
    _sc_label_db = MongoDBSCLabel()

    _redis = redis.from_url(RedisConfig.CONNECTION_URL, decode_responses=True)
    _signal_stream = SignalsStream(_redis)
    _exporter = SignalExporter(signal_stream=_signal_stream, db=None)

    streamer_adapter = PublishSignalAdapter(
        importer=_importer,
        exporter=_exporter,
        dex_db=_dex_db,
        klg_db=_klg_db,
        sc_label_db=_sc_label_db,
        collector_id=collector_id,
        batch_size=batch_size,
        max_workers=max_workers,
        query_batch_size=query_batch_size,
        forks=forks if forks is None else forks.split(','),
        chain_id=chain_id
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        exporter=_exporter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        block_batch_size=block_batch_size,
        pid_file=pid_file,
        stream_id=stream_id
    )
    streamer.stream()
