import click

from src.clis.agents.analysis_posting import analysis_posting
from src.clis.agents.news_posting import news_posting
from src.clis.load_data.agents_token_transfering import agents_token_transfering
from src.clis.load_data.news_crawler import news_crawling
from src.clis.load_data.project_change_ranking import project_change_ranking
from src.clis.load_data.project_mention_ranking import project_mention_ranking
from src.clis.load_data.token_change_ranking import token_change_ranking
from src.clis.load_data.token_mention_ranking import token_mention_ranking


@click.group()
@click.version_option(version="1.0.0")
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(news_posting, "news_posting")
cli.add_command(analysis_posting, "analysis_posting")
cli.add_command(token_change_ranking, "token_change_ranking")
cli.add_command(project_change_ranking, "project_change_ranking")
cli.add_command(token_mention_ranking, "token_mention_ranking")
cli.add_command(project_mention_ranking, "project_mention_ranking")
cli.add_command(agents_token_transfering, "agents_token_transfering")
cli.add_command(news_crawling, "news_crawling")
