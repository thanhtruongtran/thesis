import time
import click

from src.constants.config import NetworkConfig
from src.jobs.core.smart_money import UpdateProfitableTraderJob
from src.utils.logger import get_logger

logger = get_logger('Most Profitable Trader Updater')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-t', '--time-interval', default=None, type=int, help='Time interval')
@click.option('-i', '--interval', default=86400, type=int, help='Interval to run')
@click.option('-d', '--delay', default=1, show_default=True, type=int, help='Time (in seconds) to delay')
@click.option('-r', '--run-now', default=True, show_default=True, type=bool, help='False to wait until interval then run')
@click.option('-c', '--chain-id', default='0x1', type=str, help='Chain ID')
def profitable_traders_updater(time_interval, interval, delay, run_now, chain_id):
    start_time = int(time.time())

    if time_interval not in [1, 3, 7, 30]:
        logger.info("Not available time interval")

    if chain_id not in NetworkConfig.chain_id_to_db_prefix:
        logger.info("Invalid Chain ID")
        raise
    else:
        db_prefix = NetworkConfig.chain_id_to_db_prefix[chain_id]

    job = UpdateProfitableTraderJob(
        time_interval=time_interval,
        interval=interval,
        delay=delay,
        run_now=run_now,
        chain_id=chain_id,
        db_prefix=db_prefix,
    )
    job.run()

    end_time = int(time.time())
    logger.info(f"Job took {end_time - start_time}s")