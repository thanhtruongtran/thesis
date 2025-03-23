import click

from src.constants.time import TimeConstants

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-i",
    "--interval",
    default=TimeConstants.A_DAY,
    show_default=True,
    type=int,
    help="Interval to repeat the job",
)
@click.option(
    "-de",
    "--delay",
    default=TimeConstants.A_DAY,
    show_default=True,
    type=int,
    help="Time (in seconds) to delay",
)
@click.option(
    "-r",
    "--run-now",
    default=True,
    show_default=True,
    type=bool,
    help="False to wait until interval then run",
)
@click.option(
    "-ti",
    "--time-interval",
    default=3,
    show_default=True,
    type=int,
    help="Time interval to get news",
)
def news_crawling(interval, delay, run_now, time_interval):
    from src.services.crawler.news.crawling import NewsCrawling

    job = NewsCrawling(interval=interval, delay=delay, run_now=run_now, time_interval=time_interval)
    job.run()
