from sanic import Blueprint

from src.apis.feed_blueprint import bp as feed_bp
from src.apis.chat_blueprint import bp as chat_bp

api = Blueprint.group(
    feed_bp,
    chat_bp,
)