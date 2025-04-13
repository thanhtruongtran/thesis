from sanic import Sanic

from src.misc.log import log


def register_listeners(sanic_app: Sanic):
    from src.hooks.setup_connections import setup_db, setup_cache
    from src.hooks.background_tasks import add_background_tasks

    sanic_app.register_listener(setup_db, event="before_server_start")
    sanic_app.register_listener(setup_cache, event="before_server_start")

    sanic_app.register_listener(add_background_tasks, event="before_server_start")


def register_extensions(sanic_app: Sanic):
    sanic_app.config.CORS_ORIGINS = "*"


def register_hooks(sanic_app: Sanic):
    from src.hooks.request_context import after_request

    sanic_app.register_middleware(after_request, 'response')


def create_app(*config_cls) -> Sanic:
    log(message='Sanic application initialized with {}'.format(', '.join([config.__name__ for config in config_cls])),
        keyword='INFO')

    sanic_app = Sanic(__name__)

    for config in config_cls:
        sanic_app.config.update_config(config)

    register_extensions(sanic_app)
    # register_views(sanic_app)
    register_listeners(sanic_app)
    register_hooks(sanic_app)

    return sanic_app