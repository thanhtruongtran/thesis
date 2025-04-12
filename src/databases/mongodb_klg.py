import pymongo
from pymongo import MongoClient

from src.constants.config import MongoDBKLGConfig
from src.utils.logger import get_logger

logger = get_logger("MongoDB KLG")


class MongoDBKLG:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBKLGConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)

        self._db = self.connection[MongoDBKLGConfig.DATABASE]
        self._config_col = self._db["configs"]
        self._multichain_wallets_col = self._db["multichain_wallets"]
        self._smart_contracts_col = self._db["smart_contracts"]
        self.wallets = self._db["wallets"]
        self._projects_col = self._db["projects"]

    def get_price_change_logs(self, chain_id, token_addresses):
        _token_ids = [f"{chain_id}_{address}" for address in token_addresses]
        _filter = {"_id": {"$in": _token_ids}, "priceChangeLogs": {"$exists": 1}}
        _projection = ["priceChangeLogs"]
        return self._smart_contracts_col.find(filter=_filter, projection=_projection)

    def get_largest_flag(self):
        query = {"flagged": {"$exists": "True"}}
        projection = {"flagged": 1}
        cursor = (
            self.wallets.find(query, projection=projection)
            .sort("flagged", pymongo.DESCENDING)
            .limit(1)
        )
        for cur in cursor:
            largest_flag = cur["flagged"]
        return largest_flag

    def get_tokens(self, col_name: str):
        token_cursor = self._db[col_name].find(
            {
                "symbol": {"$exists": True},
            },
            {"symbol": 1},
        )

        token_lst = []
        for tok in token_cursor:
            if tok["symbol"].strip() not in token_lst:
                token_lst.append(tok["symbol"].strip())

        return token_lst

    def get_name_of_symbol(self, symbol: str, col_name: str):
        cursor = self._db[col_name].find(
            {
                "symbol": symbol,
            },
            {"name": 1},
        )

        lst_name = []
        for token in cursor:
            if token.get("name") not in lst_name:
                lst_name.append(token.get("name"))

        return lst_name

    def mapping_address_token(self, _id):
        token_info = self._db["smart_contracts"].find_one({"_id": _id}, {"symbol": 1})
        if "symbol" in token_info:
            return token_info["symbol"]

        return ""
    
    def get_token_information(self, chain_id, token_addr):
        doc_klg = self._smart_contracts_col.find_one({
            '_id': f'{chain_id}_{token_addr}',
        }, projection={
            'decimals': 1,
            'symbol': 1,
            'marketCap': 1
        })
        if doc_klg is None:
            return None, None, None
        if 'marketCap' not in doc_klg or doc_klg['marketCap'] <= 0:
            return None, None, None
        market_cap = doc_klg['marketCap']

        decimals_doc = None
        if 'decimals' in doc_klg:
            decimals_doc = doc_klg['decimals']

        symbol_doc = None
        if 'symbol' in doc_klg:
            symbol_doc = doc_klg['symbol']

        return decimals_doc, symbol_doc, market_cap
    
    def get_tokens_by_keys(self, keys, projection):
        filter_statement = {
            "idCoingecko": {"$exists": True},
            "_id": {"$in": keys}
        }
        cursor = self._smart_contracts_col.find(filter_statement, projection)
        return cursor
    
    def get_project_by_id(self, _id, projection=None):
        projection = self.get_projection_statement(projection)

        filter_ = {"_id": _id}
        try:
            cursor = self._projects_col.find_one(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return None
    
    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return {}

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements
