import os
import time

from src.utils.file import smart_open
from src.utils.logger import get_logger

logger = get_logger('Load Streamer')


class Streamer:
    def __init__(
            self, blockchain_streamer_adapter, exporter,
            last_synced_block_file='last_synced_block.txt',
            lag=0, start_block=None, end_block=None, period_seconds=30, block_batch_size=32,
            retry_errors=True, pid_file=None, stream_id='loader_id'
    ):
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file

        if self.start_block is not None or not os.path.isfile(self.last_synced_block_file):
            init_last_synced_block_file((self.start_block or 0) - 1, self.last_synced_block_file)

        self.last_synced_block = read_last_synced_block(self.last_synced_block_file)
        self.stream_id = stream_id
        self.exporter = exporter

        self._update_start_extract_at()

    def _update_start_extract_at(self):
        self.collector = self.exporter.get_loader(self.stream_id)
        if self.collector.start_extracting_block_number is None:
            self.collector.start_extracting_block_number = self.last_synced_block
            self.collector.last_updated_at_block_number = self.last_synced_block
            self.exporter.update_loader(self.collector)

    def stream(self):
        try:
            if self.pid_file is not None:
                write_to_file(self.pid_file, str(os.getpid()))
            self._do_stream()
        finally:
            if self.pid_file is not None:
                delete_file(self.pid_file)

    def _do_stream(self):
        logger.info("Stream load data:")
        while (self.end_block is None) or (self.last_synced_block < self.end_block):
            synced_blocks = 0
            try:
                synced_blocks = self._sync_cycle()
            except Exception as e:
                logger.exception(e)
                logger.error(f'An exception occurred while syncing block data: {e}')
                self.blockchain_streamer_adapter.switch_provider()
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logger.info('Nothing to enrich. Sleeping for {} seconds...'.format(self.period_seconds))
                time.sleep(self.period_seconds)

    def _sync_cycle(self):
        current_block = self.blockchain_streamer_adapter.get_current_block_number()
        if not current_block:
            return 0
        target_block = self._calculate_target_block(current_block, self.last_synced_block)
        blocks_to_sync = max(target_block - self.last_synced_block, 0)
        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.enrich_all(self.last_synced_block + 1, target_block)
            write_last_synced_block(self.last_synced_block_file, target_block)
            self.last_synced_block = target_block

            self.collector.last_updated_at_block_number = target_block
            self.exporter.update_loader(self.collector)

        return blocks_to_sync

    def _calculate_target_block(self, current_block, last_synced_block):
        target_block = current_block - self.lag
        target_block = min(target_block, last_synced_block + self.block_batch_size)
        target_block = min(target_block, self.end_block) if self.end_block is not None else target_block
        return target_block


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass


def write_last_synced_block(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + '\n')


def init_last_synced_block_file(start_block, last_synced_block_file):
    if os.path.isfile(last_synced_block_file):
        raise ValueError(
            '{} should not exist if --start-block option is specified. '
            'Either remove the {} file or the --start-block option.'.format(last_synced_block_file, last_synced_block_file))
    write_last_synced_block(last_synced_block_file, start_block)


def read_last_synced_block(file):
    with smart_open(file, 'r') as last_synced_block_file:
        return int(last_synced_block_file.read())


def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)
