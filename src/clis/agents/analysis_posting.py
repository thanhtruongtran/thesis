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
@click.option(
    "-c",
    "--category",
    default="",
    show_default=True,
    type=str,
    help="Category of specific fields in blockchain (By Defillama)",
)
@click.option(
    "-ch",
    "--chain_id",
    default="all",
    show_default=True,
    type=str,
    help="Post about tokens of specific chain",
)
@click.option(
    "-kw",
    "keyword",
    default="",
    show_default=True,
    type=str,
    help="Input keyword post wanted to post",
)
def analysis_posting(interval, delay, run_now, category, chain_id, keyword):
    from src.jobs.core.post_analysis import PostAnalysisJob

    job = PostAnalysisJob(
        interval=interval,
        delay=delay,
        run_now=run_now,
        category=category,
        chain_id=chain_id,
        keyword=keyword,
    )
    job.run()
