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
    default=900,
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
    "-col",
    "--collection",
    default="agent_contents",
    show_default=True,
    type=str,
    help="Collection to update",
)
def status_post_updating(interval, delay, run_now, collection):
    from src.jobs.core.update_status_posts import UpdateStatusPostsJob
    
    job = UpdateStatusPostsJob(interval=interval, delay=delay, run_now=run_now, collection=collection)
    job.run()
