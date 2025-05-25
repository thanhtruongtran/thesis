import time
from src.utils.logger import get_logger
from src.databases.mongodb_community import MongoDBCommunity

logger = get_logger("User Preferences Service")

class UserPreferencesService:
    def __init__(self):
        self.db = MongoDBCommunity()
        self.collection = self.db.get_collection("user_preferences")

    def get_preferences(self, user_id: str) -> dict:
        """
        Get user preferences from database
        """
        try:
            preferences = self.collection.find_one({"userId": user_id})
            if not preferences:
                return {"preferences": None}
            return {"preferences": preferences.get("preferences")}
        except Exception as e:
            logger.error(f"Error getting preferences for user {user_id}: {str(e)}")
            raise

    def save_preferences(self, user_id: str, preferences: dict) -> None:
        """
        Save user preferences to database
        """
        try:
            # Update or insert preferences
            self.collection.update_one(
                {"userId": user_id},
                {
                    "$set": {
                        "userId": user_id,
                        "preferences": preferences,
                        "updatedAt": int(time.time())
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving preferences for user {user_id}: {str(e)}")
            raise