import click

from src.constants.time import TimeConstants


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-i",
    "--interval",
    default=TimeConstants.A_DAY / 2,
    show_default=True,
    type=int,
    help="Interval to repeat the job",
)
@click.option(
    "-de",
    "--delay",
    default=1,
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
def analysis_posting(interval, delay, run_now):
    from src.jobs.core.post_analysis import PostAnalysisJob

    job = PostAnalysisJob(
        interval=interval,
        delay=delay,
        run_now=run_now,
    )
    job.run()
