import time
from typing import Dict

from src.constants.network import Networks, Chains
from src.constants.time import TimeConstants
from src.constants.signal import DEX_LENDING_EVENTS_THRESHOLD
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_dex import MongoDBDex
from src.databases.mongodb_sc_label import MongoDBSCLabel
from src.models.liquidity_pool import LiquidityPool
from src.services.blockchain.state_query_service import StateQueryService
from src.utils.logger import get_logger

logger = get_logger('Signal Executor Abstract')


class SignalExecutorAbstract(StateQueryService):
    def __init__(self, chain_id, updated_pools, provider_uri=None):
        provider_uri = Networks.providers.get(Chains.names[chain_id])
        super().__init__(provider_uri=provider_uri)

        self.updated_pools: Dict[str, LiquidityPool] = updated_pools
        self.updated_protocols: Dict[str, dict] = {}
        self.updated_projects: Dict[str, dict] = {}
        self.updated_tokens: Dict[str, dict] = {}

        self.chain_id = chain_id

    def reset_updated_entities(self, updated_pools: Dict[str, LiquidityPool], updated_protocols: Dict[str, dict] = None,
                             updated_projects: Dict[str, dict] = None, updated_tokens: Dict[str, dict] = None):
        self.updated_pools = updated_pools
        self.updated_protocols = updated_protocols
        self.updated_projects = updated_projects
        self.updated_tokens = updated_tokens

    def execute_swap(self, input_token, input_token_price, input_token_decimals, output_token, swap_path):
        """Execute add liquidity """
        pass

    def add_liquidity_information(self, event):
        """Calculate liquidity value in USD """
        contract_address = event['contract_address']
        liquidity_pool = self.updated_pools.get(contract_address)
        if not liquidity_pool or not liquidity_pool.tokens:
            return

        tokens = liquidity_pool.tokens

        decimals0 = tokens[0].get('decimals', 18)
        decimals1 = tokens[1].get('decimals', 18)

        price0 = tokens[0].get('price') or 0
        price1 = tokens[1].get('price') or 0

        amount0 = int(event.get('amount0', 0)) / 10 ** decimals0
        amount1 = int(event.get('amount1', 0)) / 10 ** decimals1

        liquidity_value = amount0 * price0 + amount1 * price1
        liquidity_pool.metadata.update({
            'liquidityValue': liquidity_value
        })

    def add_pool_information(self, event, dex_db: MongoDBDex, klg_db: MongoDBKLG):
        """Add liquidity pool information: project, tokens (name, symbol) """
        contract_address = event['contract_address']
        event['poolId'] = contract_address
        liquidity_pool = self.updated_pools.get(contract_address)
        if liquidity_pool is None:
            pool = dex_db.get_liquidity_pool(chain_id=self.chain_id, address=contract_address)
            if (not pool) or (not pool.get('tokens')):
                return

            token_keys = [f"{self.chain_id}_{t['address']}" for t in pool['tokens']]
            tokens_cursor = klg_db.get_tokens_by_keys(token_keys, projection=['chainId', 'address', 'price', 'name', 'symbol', 'imgUrl', 'idCoinGecko'])
            tokens_cursor = list(tokens_cursor)
            tokens_price = {f'{t["chainId"]}_{t["address"]}': t.get("price") for t in tokens_cursor}
            tokens_name = {f'{t["chainId"]}_{t["address"]}': t.get("name") for t in tokens_cursor}
            tokens_symbol = {f'{t["chainId"]}_{t["address"]}': t.get("symbol") for t in tokens_cursor}
            tokens_img_url = {f'{t["chainId"]}_{t["address"]}': t.get("imgUrl") for t in tokens_cursor}
            tokens_id = {f'{t["chainId"]}_{t["address"]}': t.get("idCoinGecko", f"{self.chain_id}_{t['address']}") for t in tokens_cursor}
            for token in pool['tokens']:
                token.update({
                    'price': tokens_price.get(f'{self.chain_id}_{token["address"]}'),
                    'name': tokens_name.get(f'{self.chain_id}_{token["address"]}'),
                    'symbol': tokens_symbol.get(f'{self.chain_id}_{token["address"]}'),
                    'imgUrl': tokens_img_url.get(f'{self.chain_id}_{token["address"]}'),
                    'tokenId': tokens_id.get(f'{self.chain_id}_{token["address"]}'),
                })
            liquidity_pool = LiquidityPool.from_dict(pool)
            self.updated_pools[contract_address] = liquidity_pool

        # get project information
        if liquidity_pool.project:
            proj = klg_db.get_project_by_id(liquidity_pool.project)
            if proj:
                liquidity_pool.project = {
                    "projectId": proj.get("_id"),
                    "projectName": proj.get("name"),
                    "projectImgUrl": proj.get("imgUrl"),
                    "projectType": proj.get("category"),
                    "description": proj.get("description"),
                }

    def swap_event_information(self, event):
        contract_address = event['contract_address']
        liquidity_pool = self.updated_pools.get(contract_address)
        if not liquidity_pool or not liquidity_pool.tokens:
            return

        tokens = liquidity_pool.tokens

        decimals0 = tokens[0].get('decimals', 18)
        decimals1 = tokens[1].get('decimals', 18)

        price0 = tokens[0].get('price') or 0
        price1 = tokens[1].get('price') or 0

        amount0 = 0
        amount1 = 0

        if 'amount0' in event and 'amount1' in event:
            amount0 = int(event['amount0']) / 10 ** decimals0
            amount1 = int(event['amount1']) / 10 ** decimals1

        else:
            amount0_in = int(event.get('amount0_in', 0))
            amount0_out = int(event.get('amount0_out', 0))
            amount1_in = int(event.get('amount1_in', 0))
            amount1_out = int(event.get('amount1_out', 0))

            amount0 = amount0_in if amount0_in > 0 else amount0_out
            amount1 = amount1_in if amount1_in > 0 else amount1_out

            amount0 = amount0 / 10 ** decimals0
            amount1 = amount1 / 10 ** decimals1

        if price0 > 0 and amount0 > 0:
            swap_value_usd = amount0 * price0
        elif price1 > 0 and amount1 > 0:
            swap_value_usd = amount1 * price1
        else:
            swap_value_usd = 0

        event['swapValue'] = swap_value_usd

    def lending_event_information(self, event, sc_label: MongoDBSCLabel, klg_db: MongoDBKLG):
        contract_address = event.get('contract_address')

        # get protocol information
        protocol_info = self.updated_protocols.get(contract_address)
        if protocol_info is None:
            protocol_info = sc_label.get_protocol_by_event(event)
            if protocol_info:
                self.updated_protocols[contract_address] = protocol_info

        # get project information
        project_id = protocol_info.get('projectId')
        proj = self.updated_projects.get(project_id)
        if proj is None:
            proj = klg_db.get_project_by_id(project_id)
            if proj:
                self.updated_projects[project_id] = proj
            else:
                event['value'] = 0
                return

        event['project'] = {
            "projectId": proj.get("_id"),
            "projectName": proj.get("name"),
            "projectImgUrl": proj.get("imgUrl"),
            "projectType": proj.get("category"),
            "description": proj.get("description"),
        }

        # get token information
        token_address = protocol_info.get('tokenAddress')
        collateral_asset_address = protocol_info.get('collateralAssetAddress')
        debt_asset_address = protocol_info.get('debtAssetAddress')

        token_keys = [
            f'{self.chain_id}_{addr}' 
            for addr in [token_address, collateral_asset_address, debt_asset_address] 
            if addr
        ]
        tokens_cursor = klg_db.get_tokens_by_keys(
            keys=token_keys,
            projection=['chainId', 'address', 'price', 'name', 'symbol', 'imgUrl', 'idCoinGecko']
        )

        token_list = list(tokens_cursor)
        if not token_list:
            event['value'] = 0
            return
        
        token_map = {token['address']: token for token in token_list}
        
        if token_address:
            token_info = token_map.get(token_address)
            if not token_info:
                event['value'] = 0
                return
                
            self.updated_tokens[f'{self.chain_id}_{token_address}'] = token_info
            event['token'] = {
                'price': token_info.get('price', 0),
                'name': token_info.get('name'),
                'symbol': token_info.get('symbol'),
                'imgUrl': token_info.get('imgUrl'),
                'tokenId': token_info.get('idCoinGecko', f"{self.chain_id}_{token_address}")
            }
            
            if event.get('amount'):
                event['value'] = (token_info.get('price') or 0) * (event.get('amount') or 0)
            else:
                event['value'] = 0
        else:
            collateral_info = token_map.get(collateral_asset_address)
            debt_info = token_map.get(debt_asset_address)
            
            if not (collateral_info and debt_info):
                event['value'] = 0
                return
            
            self.updated_tokens[f'{self.chain_id}_{collateral_asset_address}'] = collateral_info
            self.updated_tokens[f'{self.chain_id}_{debt_asset_address}'] = debt_info
            
            event['collateralAsset'] = {
                'price': collateral_info.get('price', 0),
                'name': collateral_info.get('name'),
                'symbol': collateral_info.get('symbol'),
                'imgUrl': collateral_info.get('imgUrl'),
                'tokenId': collateral_info.get('idCoinGecko', f"{self.chain_id}_{collateral_asset_address}")
            }
            event['debtAsset'] = {
                'price': debt_info.get('price', 0),
                'name': debt_info.get('name'),
                'symbol': debt_info.get('symbol'),
                'imgUrl': debt_info.get('imgUrl'),
                'tokenId': debt_info.get('idCoinGecko', f"{self.chain_id}_{debt_asset_address}")
            }
            
            if event.get('liquidated_collateral_amount'):
                event['value'] = (collateral_info.get('price') or 0) * (event.get('liquidated_collateral_amount') or 0)
            else:
                event['value'] = 0

    def check_new_listing_pool(self, liquidity_pool: LiquidityPool, token_0, token_1, ignore_swap_tokens=None, klg_db=MongoDBKLG()):
        is_new_listing = False
        tokens_info = []
        pool_first_updated_at = liquidity_pool.metadata.get('firstUpdatedAt', int(time.time()))
        if pool_first_updated_at < int(time.time()) - TimeConstants.A_DAY:
            logger.debug(f'Pool {liquidity_pool.address} was created more than 1 day ago.')
            return is_new_listing, tokens_info

        # if (token_0 in ignore_swap_tokens) and (token_1 in ignore_swap_tokens):
        #     logger.debug('Pool with 2 strong tokens.')
        #     return False

        # if (token_0 not in ignore_swap_tokens) and (token_1 not in ignore_swap_tokens):
        #     logger.debug(f'Pool {liquidity_pool.address} with 2 weak tokens.')
        #     return False

        if not liquidity_pool.metadata.get('liquidityValue') or liquidity_pool.metadata.get('liquidityValue', 0) < DEX_LENDING_EVENTS_THRESHOLD['new_listing']:
            return is_new_listing, tokens_info
        
        token_cursor = klg_db.get_tokens_by_keys(
            keys=[f'{self.chain_id}_{token_0}', f'{self.chain_id}_{token_1}'], 
            projection=['name', 'symbol', 'address', 'imgUrl', 'idCoinGecko']
        )
        token_list = list(token_cursor)
        if len(token_list) != 1:
            return is_new_listing, tokens_info

        known_token = token_list[0].get("address").lower()
        new_token = token_1 if token_0.lower() == known_token else token_0
        new_token_info = self.get_token_metadata(new_token)

        tokens_info = [
            {
                'name': new_token_info.get('name'),
                'symbol': new_token_info.get('symbol'),
                'address': new_token_info.get('address'),
                'tokenId': new_token_info.get('idCoinGecko', f"{self.chain_id}_{new_token_info.get('address')}"),
            },
        ]
        is_new_listing = True

        return is_new_listing, tokens_info

    def jup_perps_event_information(self, event, klg_db: MongoDBKLG):
        """Get jup perps event information"""
        position_custody_to_asset = {
            '5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm': 'BTC',
            '7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz': 'SOL',
            'AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn': 'ETH'
        }
        assets_info = {
            'BTC': {
                'name': 'Bitcoin',
                'imgUrl': 'https://coin-images.coingecko.com/coins/images/1/large/bitcoin.png',
                'symbol': 'BTC',
                'tokenId': 'bitcoin'
            },
            'SOL': {
                'name': 'Solana',
                'imgUrl': 'https://coin-images.coingecko.com/coins/images/4128/large/solana.png',
                'symbol': 'SOL',
                'tokenId': 'solana'
            },
            'ETH': {
                'name': 'Ethereum',
                'imgUrl': 'https://coin-images.coingecko.com/coins/images/279/large/ethereum.png',
                'symbol': 'ETH',
                'tokenId': 'ethereum'
            }
        }
        asset_info = assets_info.get(position_custody_to_asset.get(event.get('positionCustody')))

        event['assets'] = [
            {
                'name': asset_info.get('name'),
                'symbol': asset_info.get('symbol'),
                'imgUrl': asset_info.get('imgUrl'),
                'tokenId': asset_info.get('tokenId'),
            }
        ]

        event['positionType'] = 'Long' if event.get('positionSide') == "1" else 'Short'

        sizeUsdDelta = event.get('sizeUsdDelta') or 0
        collateralUsdDelta = event.get('collateralUsdDelta')
        event['leverage'] = float(sizeUsdDelta) / float(collateralUsdDelta) if collateralUsdDelta else 0

        if event.get('event_type') == 'LIQUIDATEFULLPOSITIONEVENT':
            event['threshold'] = float(event.get('pnlDelta'))/1e6
        else:
            event['threshold'] = float(event.get('sizeUsdDelta'))/1e6

        project_info = self.updated_projects.get('jupiter')
        if not project_info:
            project_info = klg_db.get_project_by_id('jupiter')
            if project_info:
                self.updated_projects['jupiter'] = project_info

        event['project'] = {
            "projectId": project_info.get("_id"),
            "projectName": project_info.get("name"),
            "projectImgUrl": project_info.get("imgUrl"),
            "projectType": "Perpetual",
            "description": project_info.get("description"),
        }

    def get_buy_token_event(self, tx_hash):
        """Get buy token event"""
        pass

    def get_pool_fee(self, pool_address):
        """Get pool fee """
        pass
