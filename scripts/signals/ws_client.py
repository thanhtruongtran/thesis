import asyncio
import json
import os
import sys

sys.path.append(os.getcwd())
import websockets
from dotenv import load_dotenv
from telegram import Bot

from src.databases.mongodb_community import MongoDBCommunity

load_dotenv()


def format_signal(signal):
    signal_type = signal.get("signalType", "Unknown")
    project = signal.get("projectName", "Unknown")
    value = signal.get("value", "")
    assets = signal.get("assets", [])
    chain = signal.get("chainId", "")
    explorer = signal.get("explorerUrl", "")

    # Icon cho từng loại signal
    signal_icons = {
        "add_liquidity": "💧",
        "remove_liquidity": "💦",
        "swap": "🔄",
        "new_listing": "🆕",
        "deposit": "📥",
        "withdraw": "📤",
        "borrow": "💸",
        "repay": "💰",
        "liquidate": "🔥",
        "auto_invest": "🤖",
    }
    icon = signal_icons.get(signal_type, "🚨")

    # Format assets
    asset_str = ", ".join([a.get("symbol", "") for a in assets]) if assets else ""

    # Format message
    msg = f"{icon} <b>Signal: {signal_type}</b>\n"
    msg += f"Project: <b>{project}</b>\n"
    if value:
        msg += f"Value: <b>{value}</b>\n"
    if asset_str:
        msg += f"Assets: <b>{asset_str}</b>\n"
    if chain:
        msg += f"Chain: <b>{chain}</b>\n"
    if explorer:
        msg += f"<a href='{explorer}'>🔗 View on Explorer</a>"
    return msg


async def listen_websocket():
    uri = os.getenv("WEBSOCKET_URL")
    bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    mongodb = MongoDBCommunity()

    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            if message in ["ping", "pong"]:
                print(f"Ping: {message}")
                continue

            signal = json.loads(message)
            print(f"Received from server: {signal}")

            chat_ids = []
            for doc in mongodb._db["telegram_users"].find({}):
                chat_ids.append(doc["chat_id"])

            if len(chat_ids) > 0:
                for chat_id in chat_ids:
                    try:
                        await bot.send_message(
                            chat_id=chat_id, text=format_signal(signal), parse_mode="HTML"
                        )
                    except Exception as e:
                        print(f"Send to {chat_id} failed: {e}")


async def main():
    clients = [listen_websocket(), listen_websocket()]
    await asyncio.gather(*clients)


if __name__ == "__main__":
    asyncio.run(listen_websocket())
