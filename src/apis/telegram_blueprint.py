import os
import sys

sys.path.append(os.getcwd())

from dotenv import load_dotenv

from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update
from src.databases.mongodb_community import MongoDBCommunity

load_dotenv()

db = MongoDBCommunity()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    wallet_address = context.args[0] if context.args else None

    db.update_docs(
        "telegram_users",
        [
            {
                "_id": chat_id,
                "chat_id": chat_id,
                "wallet_address": wallet_address
            }
        ]
    )
    await update.message.reply_text(
        f"You have connected to the bot successfully!\nWallet: {wallet_address}\nYou will receive signals here."
    )

app = Application.builder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()
app.add_handler(CommandHandler("start", start))
app.run_polling()