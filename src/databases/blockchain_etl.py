from pymongo import MongoClient, UpdateOne, DeleteMany
from pymongo.errors import BulkWriteError

from src.constants.config import BlockchainETLConfig
from src.constants.blockchain_etl import BlockchainETLCollections, BlockchainETLIndexes
from src.constants.time import TimeConstants
from src.utils.logger import get_logger
from src.utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('Blockchain ETL')


class BlockchainETL:
    def __init__(self, connection_url=None, db_prefix=""):
        self._conn = None
        if not connection_url:
            connection_url = BlockchainETLConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        if db_prefix:
            self.db_name = db_prefix + "_" + BlockchainETLConfig.DATABASE
        else:
            self.db_name = BlockchainETLConfig.DATABASE

        self.mongo_db = self.connection[self.db_name]

        self.block_collection = self.mongo_db[BlockchainETLCollections.blocks]
        self.transaction_collection = self.mongo_db[BlockchainETLCollections.transactions]
        self.internal_transaction_collection = self.mongo_db['internal_transactions']
        self.collector_collection = self.mongo_db[BlockchainETLCollections.collectors]
        self.lending_events_collection = self.mongo_db['lending_events']
        self.events_collection = self.mongo_db['events']
        self.dex_events_collection = self.mongo_db['dex_events']
        self.projects_collection = self.mongo_db['projects']
        self.logs_collection = self.mongo_db['logs']
        self.jup_perps_events_collection = self.mongo_db['jup-perps-events']

    def open(self):
        pass

    def close(self):
        pass

    def _create_index(self):
        # If blockchain_etl 30 days, create index
        # Collection: blocks
        # - number: -1
        #
        # Collection: transactions
        # - block_number: -1
        # - block_timestamp: 1
        # - from_address: 1, block_number: -1
        # - to_address: 1, block_number: -1
        # - from_address: 1, to_address: 1

        if BlockchainETLIndexes.ttl_blocks not in self.block_collection:
            self.block_collection.create_index([('item_timestamp', 1)], expireAfterSeconds=TimeConstants.DAYS_30,
                                               name=BlockchainETLIndexes.ttl_blocks)
        if BlockchainETLIndexes.ttl_transactions not in self.transaction_collection:
            self.transaction_collection.create_index([('item_timestamp', 1)], expireAfterSeconds=TimeConstants.DAYS_30,
                                                     name=BlockchainETLIndexes.ttl_transactions)

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return {}

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    def get_chain_block_number_by_timestamp_using_number_index(self, prefix, target_timestamp):
        """
        Get the block number for the given timestamp using binary search.
        Utilizes the collection `blocks`.
        """
        db_name = prefix + "_" + BlockchainETLConfig.DATABASE
        mongo_db = self.connection[db_name]
        block_collection = mongo_db[BlockchainETLCollections.blocks]
        # Get the latest and earliest block numbers
        latest_block = block_collection.find_one(
            filter={}, sort=[("number", -1)],
            projection={"number": 1}
        )
        earliest_block = block_collection.find_one(
            filter={}, sort=[("number", 1)],
            projection={"number": 1}
        )

        if not latest_block or not earliest_block:
            return None

        latest_block = latest_block["number"]
        earliest_block = earliest_block["number"]

        # Perform binary search within the block range
        while earliest_block <= latest_block:
            middle_block = (earliest_block + latest_block) // 2

            # Fetch the block_timestamp for the middle_block
            middle_doc = block_collection.find_one(
                {"number": {"$gte": middle_block}},
                sort=[("number", 1)],  # Sort ascending to get the earliest block >= middle_block
                projection={"timestamp": 1, "number": 1}
            )

            if not middle_doc:
                return None  # If no document matches, terminate the search.

            middle_timestamp = middle_doc["timestamp"]

            # Binary search logic
            if middle_timestamp < target_timestamp:
                # Search in the later half
                earliest_block = middle_block + 1
            elif middle_timestamp > target_timestamp:
                # Search in the earlier half
                latest_block = middle_block - 1
            else:
                # Exact match found
                return middle_doc["number"]

        # Return the closest block after completing the search
        return earliest_block


    def get_last_block_number(self, collector_id="streaming_collector"):
        """Get the last block number collected by collector"""
        last_block_number = self.collector_collection.find_one({"_id": collector_id})
        return last_block_number["last_updated_at_block_number"]

    def get_transactions_by_smart_contracts(self, from_block, to_block, contract_addresses: list):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lte": to_block}},
                {"to_address": {"$in": [address.lower() for address in contract_addresses]}},
                {"receipt_status": 1}
            ]
        }
        projection = ['from_address', 'to_address', 'input', 'block_timestamp', 'hash']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_to_addresses(self, to_addresses, from_block, to_block, projection=None):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lt": to_block}},
                {"to_address": {"$in": [address.lower() for address in to_addresses]}}
            ]
        }

        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_by_addresses(self, addresses, from_block):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block}},
                {"to_address": {"$in": [address.lower() for address in addresses]}},
                {"from_address": {"$in": [address.lower() for address in addresses]}}
            ]
        }
        projection = ['from_address', 'to_address']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_internal_txs_in_range_block_number(self, from_block, to_block):
        filter_ = {"block_number": {"$gt": from_block, "$lte": to_block}}

        cursor = self.mongo_db["internal_transactions"].find(filter_).batch_size(10000)
        return cursor

    def get_txs_in_range_block_number(self, from_block, to_block, projection=None):
        filter_ = {"block_number": {"$gt": from_block, "$lte": to_block}}
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_txs_in_range_block_number_v2(self, from_block, to_block, projection=None):
        filter_ = {"block_number": {"$gte": from_block, "$lt": to_block}}
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_block_in_range_block_number(self, from_block, to_block):
        filter_ = {"number": {"$gt": from_block, "$lte": to_block}}
        cursor = self.block_collection.find(filter_).batch_size(10000)
        return cursor

    def get_timestamp_in_range_block_number(self, from_block, to_block):
        filter_ = {"number": {"$gt": from_block, "$lte": to_block}}
        projection = {'timestamp': 1, 'number': 1}
        cursor = self.block_collection.find(filter=filter_, projection=projection)
        res = {}
        for doc in cursor:
            res[doc.get('number')] = doc.get('timestamp')

        return res

    def get_timestamp_of_block_number(self, number):
        filter_ = {'number': number}

        doc = self.block_collection.find_one(filter_, projection={'timestamp': 1})

        return doc.get('timestamp', None)

    def get_blocks(self, block_numbers):
        return self.block_collection.find({'number': {'$in': block_numbers}})

    def get_block_by_block_numbers(self, block_numbers, projection=None):
        projection = self.get_projection_statement(projection)
        cursor = self.block_collection.find({"number": {"$in": block_numbers}}, projection)
        return cursor

    def get_sort_txs_in_range(self, start_timestamp, end_timestamp):
        filter_ = {
            'block_timestamp': {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }
        }
        projection = ["from_address", "to_address", "input"]
        cursor = self.transaction_collection.find(filter_, projection).batch_size(10000)
        return cursor

    def get_native_transfer_txs(self, from_block, to_block):
        filter_ = {
            "$and": [
                {"block_number": {"$gte": from_block, "$lte": to_block}},
                {"input": "0x"},
                {"value": {"$ne": "0"}},
                {"receipt_status": 1}
            ]
        }
        projection = ['from_address', 'to_address', 'value', 'block_timestamp', 'hash']
        cursor = self.transaction_collection.find(filter_, projection=projection).batch_size(10000)
        return cursor

    def get_deploy_transactions(self, start_block, end_block, projection):
        projection = self.get_projection_statement(projection)
        filter_ = {
            "$and": [
                {"block_number": {"$gte": start_block, "$lte": end_block}},
                {"to_address": None}
            ]
        }

        cursor = self.transaction_collection.find(filter_, projection=projection)
        return cursor

    def get_block_number_by_timestamp(self, timestamp):
        filter_ = {
            'timestamp': {'$lte': timestamp}
        }
        cursor = self.block_collection.find(filter_, projection=['number', 'timestamp']).sort('timestamp', -1).limit(1)
        docs = list(cursor)
        if not docs:
            return None

        return docs[0]['number']

    def get_block_number_by_timestamp_tx(self, timestamp):
        filter_ = {
            'block_timestamp': {'$lte': timestamp}
        }
        cursor = self.transaction_collection.find(filter_, projection=['block_number', 'block_timestamp']).sort('block_timestamp', -1).limit(1)
        docs = list(cursor)
        if not docs:
            return None

        return docs[0]['block_number']

    def get_block_number_by_timestamp_using_number_index(self, target_timestamp):
        """
        Get the block number for the given timestamp using binary search.
        Utilizes the collection `blocks`.
        """
        # Get the latest and earliest block numbers
        latest_block = self.block_collection.find_one(
            filter={}, sort=[("number", -1)],
            projection={"number": 1}
        )
        earliest_block = self.block_collection.find_one(
            filter={}, sort=[("number", 1)],
            projection={"number": 1}
        )

        if not latest_block or not earliest_block:
            return None

        latest_block = latest_block["number"]
        earliest_block = earliest_block["number"]

        # Perform binary search within the block range
        while earliest_block <= latest_block:
            middle_block = (earliest_block + latest_block) // 2

            # Fetch the block_timestamp for the middle_block
            middle_doc = self.block_collection.find_one(
                {"number": {"$gte": middle_block}},
                sort=[("number", 1)],  # Sort ascending to get the earliest block >= middle_block
                projection={"timestamp": 1, "number": 1}
            )

            if not middle_doc:
                return None  # If no document matches, terminate the search.

            middle_timestamp = middle_doc["timestamp"]

            # Binary search logic
            if middle_timestamp < target_timestamp:
                # Search in the later half
                earliest_block = middle_block + 1
            elif middle_timestamp > target_timestamp:
                # Search in the earlier half
                latest_block = middle_block - 1
            else:
                # Exact match found
                return middle_doc["number"]

        # Return the closest block after completing the search
        return earliest_block

    def get_block_number_from_timestamp_by_dex_events_collection(self, target_timestamp):
        # logger.info(f"Get block number of timestamp {target_timestamp}")

        latest_block = self.dex_events_collection.find({}).sort({'block_number': -1}).limit(1)[0]['block_number']
        earliest_block = 18037987

        # Perform binary search
        while earliest_block <= latest_block:
            middle_block = (earliest_block + latest_block) // 2
            middle_block_timestamp = self.dex_events_collection.find({'block_number': {'$gte': middle_block}}).sort({'block_number': 1}).limit(1)[0]['block_timestamp']

            if middle_block_timestamp < target_timestamp:
                earliest_block = middle_block + 1
            elif middle_block_timestamp > target_timestamp:
                latest_block = middle_block - 1
            else:
                # Exact timestamp match found
                return middle_block

        return earliest_block

    def get_blocks_in_range(self, start_block, end_block):
        filter_ = {
            'number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        cursor = self.block_collection.find(filter_).batch_size(10000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_transactions_in_range(self, start_block, end_block, projection=None):
        filter_ = {
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        cursor = self.transaction_collection.find(filter_, projection).batch_size(10000)
        return cursor

    def get_transactions_in_range_block_timestamp(self, start_time, end_time, projection):
        projection = self.get_projection_statement(projection)
        filter_ = {
            'block_timestamp': {
                "$gte": start_time,
                "$lt": end_time
            }
        }
        cursor = self.transaction_collection.find(filter_, projection).batch_size(10000)
        return cursor

    def get_solana_events_in_range_block_timestamp(self, from_block, to_block, contract_addresses=None, projection=None):
        projection = self.get_projection_statement(projection)
        filter_ = {
            'block_number': {
                "$gte": from_block,
                "$lt": to_block
            },
            'program_id':{
                '$in': contract_addresses
            }
        }
        cursor = self.events_collection.find(filter_, projection).batch_size(10000)
        return cursor

    def insert_transactions(self, transactions):
        try:
            self.transaction_collection.insert_many(transactions)
        except BulkWriteError:
            data = []
            for tx in transactions:
                data.append(UpdateOne({'_id': tx['_id'], 'block_number': tx['block_number']}, {'$set': tx}, upsert=True))

            try:
                self.transaction_collection.bulk_write(data)
            except BulkWriteError as err:
                errors = err.details.get('writeErrors', [])
                if errors[0].get('code') == 11000:
                    logger.error(f'Ignore Err DuplicateKey: {errors[0].get("errmsg")}')
                else:
                    raise err

    def insert_internal_transactions(self, transactions):
        try:
            self.internal_transaction_collection.insert_many(transactions)
        except BulkWriteError:
            data = []
            for tx in transactions:
                data.append(UpdateOne({'_id': tx['_id']}, {'$set': tx}, upsert=True))
            self.internal_transaction_collection.bulk_write(data)

    def insert_blocks(self, blocks):
        try:
            self.block_collection.insert_many(blocks)
        except BulkWriteError:
            data = []
            for block in blocks:
                data.append(UpdateOne({'_id': block['_id'], 'number': block['number']}, {'$set': block}, upsert=True))
            self.block_collection.bulk_write(data)

    def delete_blocks(self, out_date_block):
        try:
            filter_ = {'number': {"$lt": out_date_block}}
            self.block_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def delete_transactions(self, out_date_block):
        try:
            filter_ = {'block_number': {"$lt": out_date_block}}
            self.transaction_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def delete_dex_events(self, out_date_block):
        try:
            filter_ = {'block_number': {"$lt": out_date_block}}
            self.dex_events_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def delete_events(self, out_date_block):
        try:
            filter_ = {'block_number': {"$lt": out_date_block}}
            self.events_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def update_ton_events(self, events):
        if self.db_name != "ton_blockchain_etl":
            logger.exception("This function only support ton network")
            return

        try:
            bulk_operations = [UpdateOne({"_id": event["_id"], 'timestamp': event['timestamp']}, {"$set": event}, upsert=True) for event in events]
            self.events_collection.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def get_ton_events_between_timestamp(self, start_timestamp, end_timestamp, project, projection=None):
        if self.db_name != "ton_blockchain_etl":
            logger.exception("This function only support ton network")
            return

        projection = self.get_projection_statement(projection)
        filter_ = {
            'timestamp': {
                '$gte': start_timestamp,
                '$lte': end_timestamp
            },
            'project': project
        }

        cursor = self.events_collection.find(filter_, projection)
        return cursor

    def delete_logs(self, out_date_block):
        try:
            filter_ = {'block_number': {"$lt": out_date_block}}
            self.logs_collection.delete_many(filter_)
        except Exception as ex:
            logger.exception(ex)

    def get_collector(self, collector_id):
        try:
            collector = self.collector_collection.find_one({'_id': collector_id})
            return collector
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_collector(self, collector):
        try:
            self.collector_collection.update_one({'_id': collector['_id']}, {'$set': collector}, upsert=True)
        except Exception as ex:
            logger.exception(ex)

    def get_the_first_tx(self, address):
        filter_ = {
            "$or": [
                {"from_address": address},
                {"to_address": address}
            ]
        }
        projection = ['block_timestamp']
        cursor = self.transaction_collection.find(filter_, projection=projection).sort('block_number').limit(1)
        return list(cursor)

    def export_collection_items(self, db, collection, operations_data):
        if not operations_data:
            logger.debug("Error: Don't have any data to write")
            return
        bulk_operations = [UpdateOne({'_id': data['_id']}, {"$set": data}, upsert=True) for data in operations_data]
        try:
            self.connection[db][collection].bulk_write(bulk_operations)
        except Exception as bwe:
            logger.error(f"Error: {bwe}")

    def get_documents(self, database, collection, filter_=None):
        if filter_ is None:
            filter_ = {}

        return self.connection[database][collection].find(filter_)

    def get_lending_events_with_wallets(self, addresses, start_block, end_block, projection=None):
        filter_ = {
            'wallet': {'$in': addresses},
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        cursor = self.lending_events_collection.find(filter_, projection)
        return list(cursor)

    def get_lending_events(self, start_block, end_block, event_type=None, projection=None):
        filter_ = {
            'block_number': {
                "$gte": start_block,
                "$lte": end_block
            }
        }
        if event_type is not None:
            filter_['event_type'] = event_type

        cursor = self.lending_events_collection.find(filter_, projection)
        return list(cursor)

    #######################
    #       Events        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_events(self, contract_addresses, from_block, to_block, projection=None, event_types=None):
        filter_ = {
            'contract_address': {'$in': contract_addresses},
            'block_number': {'$gte': from_block, '$lt': to_block}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        try:
            cursor = self.events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_solana_events(self, contract_addresses, from_block, to_block, projection=None, event_types=None):
        filter_ = {
            'program_id': {'$in': contract_addresses},
            'block_number': {'$gte': from_block, '$lt': to_block}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        try:
            cursor = self.events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_solana_jup_perps_events(self, start_block, end_block, event_types=None):
        filter_ = {
            'block_number': {'$gte': start_block, '$lte': end_block}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}
    
        try:
            cursor = self.jup_perps_events_collection.find(filter_)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dex_events_in_block_range(self, start_block, end_block, event_types=None, topics=None, projection=None):
        filter_ = {
            'block_number': {'$gte': start_block, '$lte': end_block}
        }
        if event_types is not None:
            filter_["event_type"] = {"$in": event_types}
        if topics is not None:
            filter_["topic"] = {"$in": topics}

        try:
            cursor = self.dex_events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []
    
    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dex_events_with_filter(self, filter_=None):
        if filter_ is None:
            filter_ = {}
        return self.dex_events_collection.find(filter_)

    def get_dex_events_in_block_range_with_sort(self, start_block, end_block, sort_condition, event_types=None, projection=None):
        filter_ = {
            'block_number': {'$gte': start_block, '$lte': end_block}
        }
        if event_types is not None:
            filter_["event_type"] = {"$in": event_types}

        try:
            cursor = self.dex_events_collection.find(filter_, projection=projection).sort(sort_condition)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dex_events_by_topics_in_block_range(self, start_block, end_block, topics, projection=None):
        filter_ = {
            'block_number': {'$gte': start_block, '$lte': end_block},
            'topic': {'$in': topics}
        }

        try:
            cursor = self.dex_events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_events_by_timestamp(
            self, contract_addresses, from_timestamp, to_timestamp, projection=None, event_types=None, rm_event_types=None):
        filter_ = {
            'contract_address': {'$in': contract_addresses},
            'block_timestamp': {'$gte': from_timestamp, '$lt': to_timestamp}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        if rm_event_types:
            filter_["event_type"] = {"$nin": rm_event_types}

        try:
            cursor = self.events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_events_by_block_numbers(self, contract_addresses, from_block, to_block, projection=None, event_types=None, rm_event_types=None):
        filter_ = {
            'contract_address': {'$in': contract_addresses},
            'block_number': {'$gte': from_block, '$lt': to_block}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        if rm_event_types:
            filter_["event_type"] = {"$nin": rm_event_types}

        try:
            cursor = self.events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_dex_events_by_block_numbers(self, contract_addresses, from_block, to_block, projection=None, event_types=None, rm_event_types=None):
        filter_ = {
            'contract_address': {'$in': contract_addresses},
            'block_number': {'$gt': from_block, '$lte': to_block}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        if rm_event_types:
            filter_["event_type"] = {"$nin": rm_event_types}

        try:
            cursor = self.dex_events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_dex_events_by_block_numbers_with_sort(self, contract_addresses, from_block, to_block, sort_condition, projection=None, event_types=None, rm_event_types=None):
        filter_ = {
            'contract_address': {'$in': contract_addresses},
            'block_number': {'$gt': from_block, '$lte': to_block}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        if rm_event_types:
            filter_["event_type"] = {"$nin": rm_event_types}

        try:
            cursor = self.dex_events_collection.find(filter_, projection=projection).sort(sort_condition)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_events_by_transaction(self, transactions, projection=None, event_types=None, rm_event_types=None):
        filter_ = {
            'transaction_hash': {'$in': transactions}
        }
        if event_types:
            filter_["event_type"] = {"$in": event_types}

        if rm_event_types:
            filter_["event_type"] = {"$nin": rm_event_types}

        try:
            cursor = self.events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_project(self, filter_={}):
        cursor = self.get_documents(self.db_name, "projects", filter_)
        for i in cursor:
            return i

    def get_projects_by_ids(self, ids, projection=None):
        filter_ = {"_id": {"$in": ids}}
        projection = self.get_projection_statement(projection)
        cursor = self.projects_collection.find(filter_, projection)

        return cursor

    def get_all_projects_in_chain(self, projection=None):
        projection = self.get_projection_statement(projection)
        cursor = self.projects_collection.find(filter={}, projection=projection)

        return cursor

    def get_project_by_id(self, project_id,  projection=None):
        projection = self.get_projection_statement(projection=projection)

        filter_ = {"_id": project_id}
        doc = self.projects_collection.find_one(filter=filter_, projection=projection)

        return doc

    def export_project(self, data):
        try:
            collection = self.mongo_db['projects']
            collection.update_one({'_id': data['_id']}, {"$set": data}, upsert=True)
        except Exception as e:
            logger.exception(e)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def prune_blocks(self, out_date_blocks, ordered=True):
        bulk_operators = [DeleteMany({'number': {'$lte': out_date_blocks[0]}})]
        for index in range(len(out_date_blocks) - 1):
            block1 = out_date_blocks[index]
            block2 = out_date_blocks[index + 1]
            _filter = {"$and": [{'number': {'$gte': block1}}, {'number': {'$lte': block2}}]}
            bulk_operators.append(DeleteMany(_filter))

        try:
            block_res = self.block_collection.bulk_write(bulk_operators, ordered=ordered)
            logger.info(f'Blocks collection deleted {block_res.deleted_count} records')
        except Exception as e:
            logger.exception(e)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def prune_transactions(self, out_date_blocks, ordered=True):
        bulk_operators = [DeleteMany({'block_number': {'$lte': out_date_blocks[0]}})]
        for index in range(len(out_date_blocks) - 1):
            block1 = out_date_blocks[index]
            block2 = out_date_blocks[index + 1]
            _filter = {"$and": [{'block_number': {'$gte': block1}}, {'block_number': {'$lte': block2}}]}
            bulk_operators.append(DeleteMany(_filter))

        try:
            tx_res = self.transaction_collection.bulk_write(bulk_operators, ordered=ordered)
            logger.info(f'Transactions collection deleted {tx_res.deleted_count} records')
        except Exception as e:
            logger.exception(e)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def prune_internal_transactions(self, out_date_blocks, ordered=True):
        bulk_operators = [DeleteMany({'block_number': {'$lte': out_date_blocks[0]}})]
        for index in range(len(out_date_blocks) - 1):
            block1 = out_date_blocks[index]
            block2 = out_date_blocks[index + 1]
            _filter = {"$and": [{'block_number': {'$gte': block1}}, {'block_number': {'$lte': block2}}]}
            bulk_operators.append(DeleteMany(_filter))

        try:
            tx_res = self.internal_transaction_collection.bulk_write(bulk_operators, ordered=ordered)
            logger.info(f'Transactions collection deleted {tx_res.deleted_count} records')
        except Exception as e:
            logger.exception(e)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def prune_logs(self, out_date_blocks, ordered=True):
        bulk_operators = [DeleteMany({'block_number': {'$lte': out_date_blocks[0]}})]
        for index in range(len(out_date_blocks) - 1):
            block1 = out_date_blocks[index]
            block2 = out_date_blocks[index + 1]
            _filter = {"$and": [{'block_number': {'$gte': block1}}, {'block_number': {'$lte': block2}}]}
            bulk_operators.append(DeleteMany(_filter))

        try:
            logs_res = self.logs_collection.bulk_write(bulk_operators, ordered=ordered)
            logger.info(f'Logs collection deleted {logs_res.deleted_count} records')
        except Exception as e:
            logger.exception(e)


    #######################
    #        Config       #
    #######################

    def get_config(self, key):
        try:
            filter_ = {'_id': key}
            config = self.collector_collection.find_one(filter_)
            return config
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_config(self, config):
        self.collector_collection.update_one({"_id": config["_id"]}, {"$set": config}, upsert=True)

