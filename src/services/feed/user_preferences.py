import time

from sentence_transformers import SentenceTransformer

from src.databases.mongodb_community import MongoDBCommunity
from src.utils.logger import get_logger

logger = get_logger("User Preferences Service")


class UserPreferencesService:
    def __init__(self):
        self.db = MongoDBCommunity()
        self.collection = self.db.get_collection("user_preferences")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

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
            # save embeddings
            preference_text = f"""This user is interested in the DeFi sector of {preferences["defiSector"]}, 
            focusing on asset types like {preferences["assetTypes"]}, and wants to follow topics related to {preferences["topics"]}."""
            embedding = self.model.encode(preference_text, convert_to_tensor=True)
            # Convert tensor to list for MongoDB storage
            embedding_list = embedding.cpu().numpy().tolist()
            # Update or insert preferences
            self.collection.update_one(
                {"userId": user_id},
                {
                    "$set": {
                        "userId": user_id,
                        "preferences": preferences,
                        "embedding": embedding_list,
                        "updatedAt": int(time.time()),
                    }
                },
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error saving preferences for user {user_id}: {str(e)}")
            raise
