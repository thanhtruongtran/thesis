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
    default=17900,
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
    "-c",
    "category",
    default="",
    show_default=True,
    type=str,
    help="False to wait until interval then run",
)
@click.option(
    "-t",
    "token",
    default="",
    show_default=True,
    type=str,
    help="Input token wanted to post",
)
def news_posting(interval, delay, run_now, category, token):
    from src.jobs.core.post_news import PostNewsJob

    job = PostNewsJob(interval=interval, delay=delay, run_now=run_now, category=category, token=token)
    job.run()
