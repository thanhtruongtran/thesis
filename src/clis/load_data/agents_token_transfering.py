import click

from src.constants.time import TimeConstants


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-i", "--interval", default=TimeConstants.A_HOUR, type=int, help="Interval"
)
@click.option(
    "-c",
    "--chains",
    default=["0x1", "0x2105", "0x38"],
    type=list,
    help="Chain id for query token transfer",
)
@click.option(
    "-b", "--batch_size", default=1200, type=int, help="Number of block per loading"
)
def agents_token_transfering(interval, chains, batch_size):
    from src.jobs.load_data.token_transfer_agents import AgentsTokenTransferJob

    agents_token_transfer_job = AgentsTokenTransferJob(
        interval=interval, chains=chains, batch_size=batch_size
    )
    agents_token_transfer_job.run()
