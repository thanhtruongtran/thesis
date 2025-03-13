import click

from src.constants.time import TimeConstants


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-u",
    "--updated_collection",
    default="entity_change_ranking",
    type=str,
    help="Collection name",
)
@click.option("-w", "--max_workers", default=4, type=int, help="Max workers for job")
@click.option(
    "-b",
    "--batch_size",
    default=100,
    type=int,
    help="Each tokens per update to collection",
)
@click.option(
    "-i", "--interval", default=TimeConstants.A_DAY, type=int, help="Sleep time"
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
def token_change_ranking(
    updated_collection, max_workers, batch_size, run_now, interval, delay
):
    from src.jobs.core.rank_token_change import TokenChangeRankingJob

    token_change_ranking_job = TokenChangeRankingJob(
        updated_collection=updated_collection,
        max_workers=max_workers,
        batch_size=batch_size,
        run_now=run_now,
        interval=interval,
        delay=delay,
    )
    token_change_ranking_job.run()
