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
    default=3600,
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
def news_ner(interval, delay, run_now):
    from src.jobs.core.ner_news import NERNewsJob
    ner_job = NERNewsJob(interval, delay, run_now)
    ner_job.run()
