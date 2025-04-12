import sys

from pymongo import MongoClient

from src.constants.config import MongoDBSCLabelConfig
from src.utils.logger import get_logger
from src.utils.protocol_mapping import mapping_protocol_by_event

logger = get_logger('Blockchain ETL')


class MongoDBSCLabel:
    def __init__(self, connection_url=None, database=MongoDBSCLabelConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBSCLabelConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self.mapping_projects = self.mongo_db["mapped_projects"]
        self.protocols = self.mongo_db["protocols"]
        self.smart_contracts = self.mongo_db['smart_contracts']
        self.projects = self.mongo_db['projects']

    @classmethod
    def get_projection_statement(cls, projection: list = None):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    def export_protocols(self, protocol):
        self.protocols.update_one({"_id": protocol["_id"]}, {"$set": protocol}, upsert=True)

    def export_project(self, project):
        self.projects.update_one({"_id": project["_id"]}, {"$set": project}, upsert=True)

    def get_projects(self, projects):
        filter_ = {"_id": {"$in": projects}}
        cursor = self.mapping_projects.find(filter_)
        return cursor

    def get_protocol_reserves_list(self, protocol_ids):
        filter_statement = {'_id': {'$in': protocol_ids}}
        cursor = self.protocols.find(filter_statement)

        protocols_reserves_list = {}
        for doc in cursor:
            reserves_list = doc.get('reservesList', {})
            protocols_reserves_list[doc['_id']] = reserves_list

        return protocols_reserves_list

    def get_all_projects(self):
        return self.projects.find({})

    def get_contracts(self, keys):
        cursor = self.smart_contracts.find({'_id': {'$in': keys}})
        return cursor

    def get_protocols(self, filter_=None):
        if filter_ is None:
            filter_ = {}
        return self.protocols.find(filter_)

    def get_protocol_by_id(self, _id, projection=None):
        projection = self.get_projection_statement(projection=projection)
        filter_ = {"_id": _id}

        cursor = self.protocols.find_one(filter=filter_, projection=projection)
        return cursor

    def get_protocols_by_ids(self, ids, projection=None):
        projection = self.get_projection_statement(projection=projection)
        filter_ = {"_id": {"$in": ids}}

        cursor = self.protocols.find(filter=filter_, projection=projection)
        return cursor

    def get_smart_contracts(self, filter_=None):
        if filter_ is None:
            filter_ = {}
        return self.smart_contracts.find(filter_)

    def get_smart_contract(self, filter_=None):
        if filter_ is None:
            filter_ = {}
        return self.smart_contracts.find_one(filter_)

    def get_doc(self, id):
        return self.mongo_db["dune_to_defiliama"].find_one({"_id": id})
    
    def get_protocol_by_event(self, event):
        protocol_docs = self.protocols.find({})

        for pool_info in protocol_docs:
            result = mapping_protocol_by_event(pool_info, event)
            if result:
                return result

        return None
