import datetime

import pymongo
from pymongo import MongoClient, UpdateOne

from src.constants.config import MongoDBDexConfig
from src.constants.mongodb import MongoDBDexCollections
from src.utils.dict import flatten_dict
from src.utils.logger import get_logger
from src.utils.retry import retry_handler
from src.utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('MongoDB DEX')


class MongoDBDex:
    def __init__(self, connection_url=None, database=MongoDBDexConfig.DATABASE):
        # Mongo DEX Data saved in Centic Cache DB
        if not connection_url:
            connection_url = MongoDBDexConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        self.client = MongoClient(connection_url)
        self.db = self.client[database]

        self._dexes_col = self.db[MongoDBDexCollections.dexes]
        self._pairs_col = self.db[MongoDBDexCollections.pairs]
        self._cexes_col = self.db[MongoDBDexCollections.cexes]
        self._nfts_col = self.db[MongoDBDexCollections.nfts]
        self._tickers_col = self.db[MongoDBDexCollections.tickers]
        self._exchanges_col = self.db[MongoDBDexCollections.exchanges]

        self._token_prices_col = self.db[MongoDBDexCollections.token_prices]
        self._token_prices_draft = self.db[MongoDBDexCollections.token_prices_draft]

        self._configs_col = self.db[MongoDBDexCollections.configs]

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Pairs index
        pairs_col_indexes = self._pairs_col.index_information()
        if 'pairs_index' not in pairs_col_indexes:
            self._pairs_col.create_index(
                [('lastInteractedAt', pymongo.ASCENDING), ('liquidityValueInUSD', pymongo.ASCENDING)],
                name='pairs_index', background=True
            )

    #######################
    #        DEXes        #
    #######################

    @retry_handler
    def update_dexes(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._dexes_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_dexes(self, projection=None):
        try:
            cursor = self._dexes_col.find(filter={}, projection=projection, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_dexes_by_chain(self, chain_id, project_id=None, projection=None):
        try:
            if not project_id:
                cursor = self._dexes_col.find(filter={"chainId": chain_id}, projection=projection, batch_size=1000)
                return cursor
            if project_id:
                cursor = self._dexes_col.find_one(filter={"_id": f"{chain_id}_{project_id}"}, projection=projection, batch_size=1000)
                return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    #######################
    #        Pairs        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_pairs(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._pairs_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs(self, chain_id, project_id, limited=None, last_interacted_at=None, batch_size=1000):
        try:
            filter_statement = {'chainId': chain_id, 'project': project_id}
            if last_interacted_at is not None:
                filter_statement.update({'lastInteractedAt': {'$gt': last_interacted_at}})

            if not limited:
                cursor = self._pairs_col.find(filter=filter_statement, batch_size=batch_size)
                return cursor
            else:
                cursor = self._pairs_col.find(filter=filter_statement, batch_size=batch_size).sort("liquidityValueInUSD", pymongo.DESCENDING).limit(limited)
                return cursor

        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_by_chain(self, chain_id, projection=None, batch_size=1000):
        try:
            cursor = self._pairs_col.find(filter={'chainId': chain_id}, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_pairs_with_projects(self, chain_id, project_ids: list, last_interacted_at=None, batch_size=1000):
        try:
            filter_statement = {'chainId': chain_id, 'project': {"$in": project_ids}}
            if last_interacted_at is not None:
                filter_statement.update({'lastInteractedAt': {'$gt': last_interacted_at}})

            cursor = self._pairs_col.find(filter=filter_statement, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_with_addresses(self, chain_id, addresses, projection=None, batch_size=1000):
        try:
            keys = [f'{chain_id}_{address}' for address in addresses]
            filter_statement = {'_id': {'$in': keys}}
            cursor = self._pairs_col.find(filter=filter_statement, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_pairs_with_pool_ids(self, chain_id, pool_ids, projection=None, batch_size=1000):
        try:
            filter_statement = {'poolId': {'$in': pool_ids}, 'chainId': chain_id}
            cursor = self._pairs_col.find(filter=filter_statement, projection=projection, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_pair_with_address(self, chain_id, address):
        try:
            filter_statement = {'_id':  f'{chain_id}_{address}'}
            cursor = self._pairs_col.find_one(filter=filter_statement)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_pairs_with_tokens(self, chain_id, tokens, projects=None, projection=None):
        filter_statement = {
            'chainId': chain_id,
            'tokens': {
                '$elemMatch': {
                    'address': {'$in': tokens}
                }
            },
            'liquidityValueInUSD': {'$gt': 100000}
        }

        if projects is not None:
            filter_statement['project'] = {'$in': projects}
        cursor = self._pairs_col.find(filter_statement, projection=projection)
        return cursor
    
    def get_liquidity_pool(self, chain_id, address, projection=None):
        filter_statement = {'_id': f'{chain_id}_{address}'}

        doc = self._pairs_col.find_one(filter_statement, projection=projection)
        return doc

    #######################
    #        CEXes        #
    #######################

    @retry_handler
    def update_cexes(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._cexes_col.bulk_write(bulk_operations)

    #######################
    #        NFTs         #
    #######################

    @retry_handler
    def update_nfts(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._nfts_col.bulk_write(bulk_operations)

    #######################
    #       Tickers       #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_tickers(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._tickers_col.bulk_write(bulk_operations)

    #######################
    #      Exchanges      #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_exchanges(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
        self._exchanges_col.bulk_write(bulk_operations)

    ######################
    #    Token prices    #
    ######################

    def save_token_prices(self, tokens, chain_id, timestamp):
        operators = []
        for token in tokens:
            for dex_id, price_info in token.get('priceOnExchanges', {}).items():
                if price_info and (price_info['price'] is not None):
                    operators.append({
                        'metadata': {'exchangeId': dex_id, 'chainId': chain_id, 'tokenAddress': token['address']},
                        'timestamp': datetime.datetime.fromtimestamp(timestamp).astimezone(datetime.timezone.utc),
                        'price': price_info['price']
                    })

        self._token_prices_col.insert_many(operators, ordered=False)

    def save_token_prices_draft(self, datas, chain_id, timestamp):
        operators = []
        for data in datas:
            operators.append({
                'metadata': {'chainId': chain_id, 'tokenAddress': data['address']},
                'timestamp': datetime.datetime.fromtimestamp(timestamp).astimezone(datetime.timezone.utc),
                'price': data['price']
            })

        self._token_prices_draft.insert_many(operators, ordered=False)


    def get_token_prices_in_range(self, tokens, project, chain_id, start_time, end_time):
        filter_ = {
            'metadata.tokenAddress': {"$in": tokens},
            'metadata.exchangeId': project,
            'metadata.chainId': chain_id,
            "timestamp": {
                "$gte": start_time,
                "$lte": end_time
            }
        }

        cursor = self._token_prices_col.find(filter_)
        return cursor

    def get_price_by_timestamp(self, timestamp, chain_id, token_addr):
        if token_addr == '0x57e114b691db790c35207b2e685d4a43181e6061':
            return None
        tmp_timestamp = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        query = {
            'metadata.tokenAddress': token_addr,
            'metadata.chainId': chain_id,
            'timestamp': {
                '$lte': tmp_timestamp
            },
            'price': {'$ne': None}
        }
        zz = self._token_prices_col.find(query).sort({'timestamp': -1}).limit(1).explain()
        print(zz)
        cursor = self._token_prices_col.find(query).sort({'timestamp': -1}).limit(1)
        try:
            price = cursor[0]['price']
            logger.info(f'Price {token_addr} - {timestamp} - {price}')
            return price
        except Exception as e:
            logger.info(f'Price {token_addr} - {timestamp} - {e}')
            return None

    #######################
    #       Configs       #
    #######################

    def get_config(self, key):
        try:
            config = self._configs_col.find_one({'_id': key})
            return config
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_config(self, config, merge=True):
        try:
            if merge:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": flatten_dict(config)}, upsert=True)]
            else:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": config}, upsert=True)]
            self._configs_col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def get_last_start_timestamp_for_token_price_swap_event(self,key):
        try:
            doc_start = self._configs_col.find_one({'_id': key}, {'_id': 0, 'timestamp': 1})
            return doc_start['timestamp']
        except Exception as e:
            logger.exception(e)

    def update_config_start_timestamp(self, _id, timestamp):
        try:
            self._configs_col.update_one({'_id': _id}, {'$set': {'timestamp': timestamp}})
        except Exception as e:
            logger.exception(e)
