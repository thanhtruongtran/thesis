from src.apis.signals_blueprint import publish_signals_task


async def add_background_tasks(sanic_app, _):
    sanic_app.add_task(publish_signals_task(sanic_app.ctx.async_redis))
