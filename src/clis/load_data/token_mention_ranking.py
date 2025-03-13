import click

from src.constants.time import TimeConstants


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-i", "--interval", default=TimeConstants.A_HOUR, type=int, help="Sleep time"
)
def token_mention_ranking(interval):
    from src.jobs.load_data.rank_token_mention import RankTokenMentionJob

    token_mention_ranking_job = RankTokenMentionJob(interval=interval)
    token_mention_ranking_job.run()
