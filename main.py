import os
import time
import warnings

from sanic import Request, json
from sanic_ext.extensions.openapi import openapi

from src import create_app
from src.apis import api
from src.constants.config import Config, LocalDBConfig
from src.misc.log import log
from src.utils.logger import get_logger

warnings.filterwarnings("ignore")

logger = get_logger("Main")

app = create_app(Config, LocalDBConfig)
app.ext.openapi.add_security_scheme(
    "Authorization", "apiKey", location="header", name="Authorization"
)
app.blueprint(api)


@app.route("/ping", methods={"GET"})
@openapi.exclude()
@openapi.tag("Ping")
@openapi.summary("Ping server !")
async def hello_world(request: Request):
    response = json(
        {"description": "Success", "status": 200, "message": "Hello, World !!!"}
    )
    return response


@app.middleware("request")
async def add_start_time(request: Request):
    request.headers["start_time"] = time.time()


@app.middleware("response")
async def add_spent_time(request: Request, response):
    try:
        if "start_time" in request.headers:
            timestamp = request.headers["start_time"]
            spend_time = round((time.time() - timestamp), 3)
            response.headers["latency"] = spend_time

            msg = "{status} {method} {path} {query} {latency}s".format(
                status=response.status,
                method=request.method,
                path=request.path,
                query=request.query_string,
                latency=spend_time,
            )
            if response.status >= 400:
                logger.error(msg)
            elif response.status >= 300:
                logger.warning(msg)
            else:
                logger.info(msg)
    except Exception as ex:
        logger.exception(ex)


if __name__ == "__main__":
    if "SECRET_KEY" not in os.environ:
        log(
            message="SECRET KEY is not set in the environment variable.", keyword="WARN"
        )

    try:
        app.run(**app.config["RUN_SETTING"])
    except (KeyError, OSError):
        log("End Server...")
