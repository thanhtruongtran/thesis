import asyncio
import json as js
import time
import uuid

import base58
import redis.asyncio as redis
from sanic import Blueprint, Request, Websocket, json
from sanic_ext import validate
from sanic_ext.extensions.openapi import openapi
from websockets import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

from src.constants.signal import SignalStream
from src.constants.time import TimeConstants
from src.models.signal import WebsocketSignalsQuery
from src.databases.mongodb_community import MongoDBCommunity
from src.utils.logger import get_logger
from src.utils.time import round_timestamp
from scripts.count_database_queries import count_db_queries
from scripts.signals.example_signals import example_signal

logger = get_logger('Signals')

signals_bp = Blueprint('signals_blueprint', url_prefix='/')

connected_ws_clients = {}


@signals_bp.get('/used')
@openapi.tag("DeFAI")
@openapi.summary("Get number requests and signals")
async def get_used_requests_and_signals(request: Request):
    r: redis.Redis = request.app.ctx.async_redis

    current_time = int(time.time())
    today_timestamp = round_timestamp(current_time)
    yesterday_timestamp = today_timestamp - TimeConstants.A_DAY
    daily_queries = count_db_queries(
        start_time=yesterday_timestamp,
        end_time=current_time
    )
    number_of_requests = daily_queries.get(today_timestamp, 0) + daily_queries.get(yesterday_timestamp, 0) / TimeConstants.A_DAY * (current_time - today_timestamp)

    # Count number of signals in the last 24h
    n_signals = await count_signal_last_24h(r=r)

    return json({
        'metrics': {
            'numberOfRequests': int(number_of_requests),
            'avgRequestsPerSecond': int(number_of_requests / TimeConstants.A_DAY),
            'numberOfSignals': n_signals,
            'avgSignalsPerMinute': int(n_signals / (24 * 60)),
            'duration': TimeConstants.A_DAY
        }
    })

# @signals_bp.get('/signals')
# @openapi.tag("DeFAI")
# @openapi.summary("Get latest signals")
# async def get_latest_signals(request: Request):
#     r: redis.Redis = request.app.ctx.async_redis

#     n_signals = int(request.args.get("limit", 10))
#     chain_id = request.args.get("chainId")
#     signal_type = request.args.get("type")

#     last_id = '+'
#     signals = {}

#     messages = await r.xrevrange(SignalStream.stream_name, max=last_id, count=n_signals)

#     for entry_id, message in messages:
#         signal = js.loads(message['msg'])
#         if not check_signal_by_filter(signal, chain_id=chain_id, signal_type=signal_type, signal_filter_str=None):
#             continue
#         signals[signal.get('_id', signal.get('id'))] = signal

#     signals = list(signals.values())
#     signals.sort(key=lambda x: x['blockNumber'])
#     return json({
#         "data": signals,
#         "message": "Success"
#     })


@signals_bp.get('/signals')
@openapi.tag("DeFAI")
@openapi.summary("Get latest signals")
async def get_latest_signals(request: Request):
    mongodb: MongoDBCommunity = request.app.ctx.community_db

    cursor = mongodb._db["defi_signals"].find(
        {
            "timestamp": {"$gte": int(time.time()) - 1800},
        },
        {"_id":0}
    ).sort("timestamp", -1)
    signals = list(cursor)
    
    return json({
        "data": signals,
        "message": "Success"
    })


@validate(query=WebsocketSignalsQuery)
async def retrieve_signals_via_websocket(request: Request, ws: Websocket, query: WebsocketSignalsQuery):
    r: redis.Redis = request.app.ctx.async_redis
    ws_id = ws.__hash__()

    if query.filter:
        example_filter = "signal['value'] > 2000 and 'USDC' in [a['symbol'] for a in signal['assets']]"

        try:
            signal_filter = base58.b58decode(query.filter).decode()
            eval(signal_filter, {}, {"signal": example_signal})
        except (ValueError, TypeError):
            await ws.send(js.dumps({'error': 400, 'message': f'Invalid signal filter {query.filter}. Example to filter signal with value more than $20K and USDC assets: {example_filter}'}))
            return

    query_dict = {'chainId': query.chainId, 'type': query.type, 'filter': query.filter}
    await fetch_last_signals(ws, r=r, query=query_dict, n_signals=10)

    connected_ws_clients.update({ws_id: {'client': ws, 'query': query_dict}})
    logger.info(f'Client connected: {ws_id} (request {request.id})')

    try:
        while True:
            await ws.send('ping')
            await asyncio.sleep(10)
    finally:
        connected_ws_clients.pop(ws_id)
        logger.info(f'Client disconnected: {ws_id}')


signals_bp.add_websocket_route(retrieve_signals_via_websocket, "/signals/ws")


async def publish_signals_task(r: redis.Redis):
    name = str(uuid.uuid4())
    await create_consumer_group(r, name=name)

    try:
        last_id = '>'
        while True:
            await publish_signals(r=r, name=name, last_id=last_id)

    finally:
        await delete_consumer_group(r, name=name)


async def publish_signals(r: redis.Redis, name, last_id='>'):
    messages = await r.xreadgroup(
        groupname=name, consumername=name,
        streams={SignalStream.stream_name: last_id},
        count=10, block=10000
    )
    for s, entries in messages:
        for entry_id, message in entries:
            signal = js.loads(message['msg'])
            await broadcast_signals(signal, msg=message['msg'])

            last_id = entry_id
            await r.xack(SignalStream.stream_name, name, entry_id)

            # logger.debug(f'Signal: {signal}')
            await asyncio.sleep(0)

    return last_id


async def create_consumer_group(r: redis.Redis, name: str):
    try:
        await r.xgroup_create(SignalStream.stream_name, name, id="$", mkstream=True)
        logger.info(f"Consumer group '{name}' created.")
    except redis.ResponseError:
        logger.info(f"Consumer group '{name}' already exists.")


async def delete_consumer_group(r: redis.Redis, name: str):
    await r.xgroup_destroy(SignalStream.stream_name, name)
    logger.info(f"Consumer group '{name}' already destroyed.")


async def broadcast_signals(signal, msg=None):
    if msg is None:
        msg = js.dumps(signal)

    for ws_id, ws_info in connected_ws_clients.copy().items():
        try:
            ws: Websocket = ws_info['client']
            query = ws_info['query']
            if not check_signal_by_filter(signal, chain_id=query['chainId'], signal_type=query['type'], signal_filter_str=query['filter']):
                continue

            try:
                await ws.send(msg)
            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK):
                connected_ws_clients.pop(ws_id)
                logger.info(f'Client disconnected: {ws_id}')
        except Exception as ex:
            logger.exception(ex)


async def fetch_last_signals(ws: Websocket, r: redis.Redis, query, n_signals=10, timeout=3):
    last_id = '+'
    start_time = time.time()
    signals = {}
    while len(signals) < n_signals:
        messages = await r.xrevrange(SignalStream.stream_name, max=last_id, count=n_signals)
        for entry_id, message in messages:
            signal = js.loads(message['msg'])
            last_id = entry_id
            if not check_signal_by_filter(signal, chain_id=query['chainId'], signal_type=query['type'], signal_filter_str=query['filter']):
                continue

            signals[signal.get('_id', signal.get('id'))] = signal

        if not messages or (time.time() - start_time) > timeout:
            break
        await asyncio.sleep(0.1)

    signals = list(signals.values())
    signals.sort(key=lambda x: x['blockNumber'])
    for signal in signals:
        await ws.send(js.dumps(signal))


def check_signal_by_filter(signal, chain_id=None, signal_type=None, signal_filter_str=None):
    if chain_id is not None and chain_id != signal.get('chainId'):
        return False

    if signal_type is not None and signal_type != signal.get('signalType'):
        return False

    if signal_filter_str is not None:
        signal_filter = base58.b58decode(signal_filter_str).decode()
        if not eval(signal_filter, {}, {"signal": signal}):
            return False

    return True


async def count_signal_last_24h(r: redis.Redis):
    timestamp_threshold = int(time.time()) - TimeConstants.A_DAY
    keys = await r.keys(r'signal_count:*')

    filtered_keys = []
    for key in keys:
        timestamp = key.split(':')[-1]
        if not timestamp.isdecimal() or int(timestamp) < timestamp_threshold:
            continue

        filtered_keys.append(key)

    values = await asyncio.gather(*[r.get(k) for k in filtered_keys], return_exceptions=True)
    count = sum([int(v) for v in values if isinstance(v, str) and v.isdecimal()])

    return count
