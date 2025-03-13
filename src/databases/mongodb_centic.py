from typing import List, Optional

from pymongo import MongoClient

from src.constants.config import MongoDBCenticConfig
from src.utils.logger import get_logger

logger = get_logger("MongoDB Centic")


class CenticDBCollections:
    projects = "projects"


class MongoDBCentic:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBCenticConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        self.connection_url_not_split = connection_url
        self.connection = MongoClient(connection_url)
        self.client: Optional[MongoClient] = None
        self.db_name = MongoDBCenticConfig.DATABASE

        self._db = self.connection[MongoDBCenticConfig.DATABASE]
        self._projects = self._db["projects"]

    def _get_project_id(self):
        cursor = self._projects.find({}, {"projectId": 1})
        project_id_lst = []
        for cur in cursor:
            project_id_lst.append(cur["projectId"])
        return project_id_lst

    def get_project_social_media(self, project_id):
        _projects_coll = self.get_collection(
            mongo_collection=CenticDBCollections.projects
        )
        project = _projects_coll.find_one(
            {"projectId": project_id}, ["projectId", "settings.socialMedia"]
        )
        if not project:
            return {}

        socials = project.get("settings", {}).get("socialMedia", [])
        return format_social_media_setting(socials)

    def get_projects_social_media(self, project_ids: List[str] = None):
        filter_statement = {}
        if project_ids is not None:
            filter_statement.update({"projectId": {"$in": project_ids}})

        _projects_coll = self.get_collection(
            mongo_collection=CenticDBCollections.projects
        )
        cursor = _projects_coll.find(
            filter=filter_statement, projection=["projectId", "settings.socialMedia"]
        )

        projects = {}
        for project in cursor:
            socials = project.get("settings", {}).get("socialMedia", [])
            accounts = format_social_media_setting(socials)
            projects[project["projectId"]] = accounts

        return projects

    def get_conn(self) -> MongoClient:
        """Fetches PyMongo Client."""
        if self.client is not None:
            return self.client

        self.client = MongoClient(self.connection_url_not_split)
        return self.client

    def get_collection(self, mongo_collection: str, mongo_db: str = None):
        """
        Fetches a mongo collection object for querying.

        Uses connection schema as DB unless specified.
        """
        mongo_db = mongo_db or self.db_name
        mongo_conn: MongoClient = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)


def format_social_media_setting(socials):
    accounts = {
        "twitter_accounts": [],
        "telegram_channels": [],
        "discord_servers": [],
        "telegram_admins": {},
    }

    for social in socials:
        platform = social["platform"]
        account_id = social["id"]
        if platform == "twitter":
            accounts["twitter_accounts"].append(account_id)
        elif platform == "telegram":
            accounts["telegram_channels"].append(account_id)
            if social.get("announcement"):
                accounts["telegram_id"] = social.get("telegramId")
            if social.get("admins"):
                accounts["telegram_admins"].update({account_id: social.get("admins")})
        elif platform == "discord":
            accounts["discord_servers"].append(account_id)

    return accounts
