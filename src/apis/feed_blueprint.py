from datetime import datetime

import pandas as pd
from sanic import Blueprint, json
from sanic_ext import openapi

from src.databases.mongodb_cdp import MongoDBCDP
from src.utils.logger import get_logger


logger = get_logger("Feed Blueprint")
bp = Blueprint("feed_blueprint", url_prefix="/")