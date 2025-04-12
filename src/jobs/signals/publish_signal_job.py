import time
from typing import Dict
from multithread_processing.base_job import BaseJob

from src.constants.signal import DEX_LENDING_EVENTS_THRESHOLD, IGNORE_SWAP_TOKENS, JUP_PERPS_THRESHOLD
from src.constants.network import EMPTY_TOKEN_IMG
from src.constants.network import Scans
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_dex import MongoDBDex
from src.databases.mongodb_sc_label import MongoDBSCLabel
from src.models.liquidity_pool import LiquidityPool
from src.services.blockchain.signal_executors.signal_executor_abstract import SignalExecutorAbstract
from src.streaming.exporters.signal_exporter import SignalExporter
from src.utils.logger import get_logger

logger = get_logger('Publish Signal Sync Job')


class PublishSignalJob(BaseJob):
    def __init__(
            self, start_block, end_block,
            batch_size=4, max_workers=8,
            importer: BlockchainETL = None, exporter: SignalExporter = None,
            chain_id=None, query_batch_size=2000,
            dex_db: MongoDBDex = None, klg_db: MongoDBKLG = None, sc_label_db: MongoDBSCLabel = None,
            forks=None, collector_id=["liquidity-pool-events-collector", "lending_events"],
    ):
        self.chain_id = chain_id
        self.chain_scan = Scans.scan_base_urls.get(self.chain_id, None)
        self.ignore_swap_tokens = IGNORE_SWAP_TOKENS.get(self.chain_id, [])
        
        self.importer = importer
        self.exporter = exporter

        self.query_batch_size = query_batch_size

        self.end_block = end_block
        self.start_block = start_block

        self.dex_db = dex_db
        self.klg_db = klg_db
        self.sc_label_db = sc_label_db

        self.updated_pools: Dict[str, LiquidityPool] = {}
        self.updated_protocols: Dict[str, dict] = {}
        self.updated_projects: Dict[str, dict] = {}
        self.updated_tokens: Dict[str, dict] = {}

        self.collector_id = collector_id
        self.forks = forks
        self.executor = SignalExecutorAbstract(chain_id=self.chain_id, updated_pools=self.updated_pools)

        work_iterable = range(start_block, end_block + 1)
        super().__init__(work_iterable, batch_size, max_workers)

    def _start(self):
        self.topics = {
            'uniswap-v2': {
                "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496": "Burn (v2)",
                "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f": "Mint",
            },
            'uniswap-v3': {
                "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": "Burn (v3)",
                "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": "Mint (v3)",
            }
        }
        self.topics_swap = {
            'uniswap-v2': {
                "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": "Swap (v2)",
            },
            'uniswap-v3': {
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": "Swap (Uniswap v3)",
                "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83": "Swap (PancakeSwap v3)"
            }
        }
        if self.forks is not None:
            self.topics = {fork: topics for fork, topics in self.topics.items() if fork in self.forks}

        topics_hash = set()
        topics_hash_swap = set()
        for fork, topics in self.topics.items():
            topics_hash.update(topics.keys())
        for fork, topics in self.topics_swap.items():
            topics_hash_swap.update(topics.keys())

        self.topics_hash = list(topics_hash)
        self.topics_hash_swap = list(topics_hash_swap)

        self.dex_event_types = {
            "Burn (v2)": "remove_liquidity",
            "Mint": "add_liquidity",
            "Burn (v3)": "remove_liquidity",
            "Mint (v3)": "add_liquidity",
        }
        self.swap_event_types = {
            "Swap (v2)": "swap",
            "Swap (Uniswap v3)": "swap",
            "Swap (PancakeSwap v3)": "swap",
        }
        self.lending_event_types = {
            "DEPOSIT": "deposit",
            "WITHDRAW": "withdraw",
            "BORROW": "borrow",
            "REPAY": "repay",
            "LIQUIDATE": "liquidate",
        }
        self.jup_perps_event_types = {
            "INCREASEPOSITIONEVENT": "open_position",
            "INSTANTINCREASEPOSITIONEVENT": "open_position",
            "DECREASEPOSITIONEVENT": "close_position",
            "INSTANTDECREASEPOSITIONEVENT": "close_position",
            "LIQUIDATEFULLPOSITIONEVENT": "liquidate",
        }

        self.executor.reset_updated_entities(self.updated_pools, self.updated_protocols, self.updated_projects, self.updated_tokens)
        self._signals = []

    def _end(self):
        self.batch_executor.shutdown()

        self._export()

    def _execute_dex_event(self, works):
        start_block = works[0]
        end_block = works[-1]

        events_cursor = self.importer.get_dex_events_in_block_range(
            start_block=start_block, end_block=end_block,
            topics=self.topics_hash
        )
        events = list(events_cursor)

        # Execute by dex fork
        for fork, topics in self.topics.items():
            events_by_fork = [event for event in events if event['topic'] in topics]
            
            if (not events_by_fork):
                continue

            logger.info(f'Execute fork {fork} with {len(events_by_fork)} events')
            
            for event in events_by_fork:
                # get event type
                event_type = self.dex_event_types.get(topics.get(event['topic']))
                liq_threshold = DEX_LENDING_EVENTS_THRESHOLD.get(event_type)

                self.executor.add_pool_information(event, dex_db=self.dex_db, klg_db=self.klg_db)
                self.executor.add_liquidity_information(event)

                liquidity_pool = self.updated_pools.get(event['poolId'])
                if not liquidity_pool or not liquidity_pool.project:
                    continue
                
                if event.get('block_timestamp') is None:
                    continue
                
                project = liquidity_pool.project if isinstance(liquidity_pool.project, dict) else {}
                
                is_new_listing, tokens_info = self.executor.check_new_listing_pool(
                    liquidity_pool, 
                    liquidity_pool.tokens[0].get('address'), 
                    liquidity_pool.tokens[1].get('address')
                )
                is_large_liquidity = liquidity_pool.metadata.get('liquidityValue', 0) > liq_threshold

                if is_large_liquidity or is_new_listing:
                    signal_type = "new_listing" if is_new_listing else event_type
                    if signal_type == "new_listing" and self.chain_id == "0x138de":
                        description = "Has been applied to AthenAI"
                    else:
                        description = ""

                    signal = {
                        'id': event['_id'],
                        'chainId': self.chain_id,
                        'timestamp': event['block_timestamp'],
                        'blockNumber': event['block_number'],
                        'transactionHash': event['transaction_hash'],
                        'contractAddress': event['contract_address'],
                        'wallet': event['wallet'],
                        "explorerUrl": f"{self.chain_scan}tx/{event['transaction_hash']}",
                        'signalType': signal_type,
                        'valueType': 'Liquidity',
                        'projectId': project.get('projectId'),
                        'projectName': project.get('projectName'),
                        'projectImgUrl': project.get('projectImgUrl'),
                        'projectType': project.get('projectType'),
                        'description': description,
                        'tags': [],
                    }

                    value = liquidity_pool.metadata.get('liquidityValue', 0)
                    signal['value'] = f"${value:,.2f}"

                    if signal_type == "new_listing":
                        signal['assets'] = [
                            {
                                "tokenId"
                                "name": tokens_info[0].get('name'),
                                "symbol": tokens_info[0].get('symbol'),
                                "imgUrl": tokens_info[0].get('imgUrl', EMPTY_TOKEN_IMG),
                                "tokenId": tokens_info[0].get('tokenId'),
                            },
                        ]
                    else:
                        signal['assets'] = [
                            {
                                "name": liquidity_pool.tokens[0].get('name'),
                                "symbol": liquidity_pool.tokens[0].get('symbol'),
                                "imgUrl": liquidity_pool.tokens[0].get('imgUrl', EMPTY_TOKEN_IMG),
                                "tokenId": liquidity_pool.tokens[0].get('tokenId'),
                            },
                            {
                                "name": liquidity_pool.tokens[1].get('name'),
                                "symbol": liquidity_pool.tokens[1].get('symbol'),
                                "imgUrl": liquidity_pool.tokens[1].get('imgUrl', EMPTY_TOKEN_IMG),
                                "tokenId": liquidity_pool.tokens[1].get('tokenId'),
                            }
                        ]

                        if not signal['assets'][0]['symbol'] and not signal['assets'][1]['symbol']:
                            continue
                        elif not signal['assets'][0]['symbol']:
                            signal['assets'].pop(0)
                        elif not signal['assets'][1]['symbol']:
                            signal['assets'].pop(1)

                    self._signals.append(signal)

                    # log signal latency
                    block_timestamp = event.get('block_timestamp')
                    if block_timestamp:
                        now_ts = int(time.time())
                        latency_sec = now_ts - int(block_timestamp)
                        logger.info(f"Latency: {latency_sec}s")

    def _execute_swap_event(self, works):
        start_block = works[0]
        end_block = works[-1]

        whale_wallets = self.exporter.get_whales_wallet(self.chain_id)
        if not whale_wallets:
            whale_wallets_cursor = self.klg_db.get_wallet_with_filter(
                filter_={
                    'chainId': self.chain_id,
                    'tags': 'whales',
                    'balanceInUSD': {'$gte': 1000000}
                }
            )
            whale_wallets = [wallet.get('address') for wallet in whale_wallets_cursor]
            self.exporter.cache_whales_wallet(whale_wallets, self.chain_id)
        
        events_cursor = self.importer.get_dex_events_with_filter(
            filter_={
                'block_number': {'$gte': start_block, '$lte': end_block},
                'topic': {'$in': self.topics_hash_swap},
                'wallet': {'$in': whale_wallets}
            }
        )
        events = list(events_cursor)

        # Execute by dex fork
        for fork, topics in self.topics_swap.items():
            events_by_fork = [event for event in events if event['topic'] in topics]
            
            if (not events_by_fork):
                continue

            logger.info(f'Execute fork {fork} with {len(events_by_fork)} events')
            
            for event in events_by_fork:
                event_type = "swap"
                liq_threshold = DEX_LENDING_EVENTS_THRESHOLD.get(event_type)
                self.executor.add_pool_information(event, dex_db=self.dex_db, klg_db=self.klg_db)
                self.executor.swap_event_information(event)

                liquidity_pool = self.updated_pools.get(event['poolId'])
                if not liquidity_pool or not liquidity_pool.project:
                    continue
                
                if event.get('block_timestamp') is None:
                    continue
                    
                project = liquidity_pool.project if isinstance(liquidity_pool.project, dict) else {}
                if event.get('swapValue', 0) > liq_threshold:
                    signal = {
                        'id': event['_id'],
                        'chainId': self.chain_id,
                        'timestamp': event['block_timestamp'],
                        'blockNumber': event['block_number'],
                        'transactionHash': event['transaction_hash'],
                        'contractAddress': event['contract_address'],
                        'wallet': event['wallet'],
                        "explorerUrl": f"{self.chain_scan}tx/{event['transaction_hash']}",
                        'signalType': event_type,
                        'valueType': 'Volume',
                        'projectId': project.get('projectId'),
                        'projectName': project.get('projectName'),
                        'projectImgUrl': project.get('projectImgUrl'),
                        'projectType': project.get('projectType'),
                        'description': "",
                        'tags': ['whale'],
                    }

                    value = event['swapValue']
                    signal['value'] = f"${value:,.2f}"

                    signal['assets'] = [
                        {
                            "name": liquidity_pool.tokens[0].get('name'),
                            "symbol": liquidity_pool.tokens[0].get('symbol'),
                            "imgUrl": liquidity_pool.tokens[0].get('imgUrl', EMPTY_TOKEN_IMG),
                            'tokenId': liquidity_pool.tokens[0].get('tokenId'),
                        },
                        {
                            "name": liquidity_pool.tokens[1].get('name'),
                            "symbol": liquidity_pool.tokens[1].get('symbol'),
                            "imgUrl": liquidity_pool.tokens[1].get('imgUrl', EMPTY_TOKEN_IMG),
                            'tokenId': liquidity_pool.tokens[1].get('tokenId'),
                        }
                    ]

                    if not signal['assets'][0]['symbol'] and not signal['assets'][1]['symbol']:
                        continue
                    elif not signal['assets'][0]['symbol']:
                        signal['assets'].pop(0)
                    elif not signal['assets'][1]['symbol']:
                        signal['assets'].pop(1)

                    self._signals.append(signal)

                    # log signal latency
                    block_timestamp = event.get('block_timestamp')
                    if block_timestamp:
                        now_ts = int(time.time())
                        latency_sec = now_ts - int(block_timestamp)
                        logger.info(f"Latency: {latency_sec}s")

    def _execute_lending_event(self, works):
        start_block = works[0]
        end_block = works[-1]
        events_cursor = self.importer.get_lending_events(
            start_block=start_block, end_block=end_block,
        )
        events = list(events_cursor)

        for event_type in self.lending_event_types:
            events_by_type = [event for event in events if event['event_type'] == event_type]
            if (not events_by_type):
                continue

            logger.info(f'Execute lending event {event_type} with {len(events_by_type)} events')

            for event in events_by_type:
                liq_threshold = DEX_LENDING_EVENTS_THRESHOLD.get(event_type.lower())
                self.executor.lending_event_information(event, sc_label=self.sc_label_db, klg_db=self.klg_db)
                if event.get('value') > liq_threshold:
                    signal = {
                        'id': event['_id'],
                        'chainId': self.chain_id,
                        'timestamp': event['block_timestamp'],
                        'blockNumber': event['block_number'],
                        'transactionHash': event['transaction_hash'],
                        'contractAddress': event['contract_address'],
                        'wallet': event['wallet'],
                        "explorerUrl": f"{self.chain_scan}tx/{event['transaction_hash']}",
                        'signalType': self.lending_event_types.get(event_type),
                        'valueType': 'Value',
                        'projectId': event.get('project').get('projectId'),
                        'projectName': event.get('project').get('projectName'),
                        'projectImgUrl': event.get('project').get('projectImgUrl'),
                        'projectType': event.get('project').get('projectType'),
                        'description': "",
                        'tags': [],
                    }

                    value = event.get('value')
                    signal['value'] = f"${value:,.2f}"

                    if event_type == "LIQUIDATE":
                        signal['extraInfo'] = {
                            "liquidator": event.get('liquidator'),
                            "debtor": event.get('user'),
                        }
                        signal['assets'] = [
                            {
                                "name": event.get('collateralAsset', {}).get('name'),
                                "symbol": event.get('collateralAsset', {}).get('symbol'),
                                "imgUrl": event.get('collateralAsset', {}).get('imgUrl', EMPTY_TOKEN_IMG),
                                "tokenId": event.get('collateralAsset', {}).get('tokenId'),
                            },
                            {
                                "name": event.get('debtAsset', {}).get('name'),
                                "symbol": event.get('debtAsset', {}).get('symbol'),
                                "imgUrl": event.get('debtAsset', {}).get('imgUrl', EMPTY_TOKEN_IMG),
                                "tokenId": event.get('debtAsset', {}).get('tokenId'),
                            }
                        ]
                    else:
                        signal['assets'] = [
                            {
                                "name": event.get('token', {}).get('name'),
                                "symbol": event.get('token', {}).get('symbol'),
                                "imgUrl": event.get('token', {}).get('imgUrl', EMPTY_TOKEN_IMG),
                                "tokenId": event.get('token', {}).get('tokenId'),
                            }
                        ]

                    self._signals.append(signal)

                    # log signal latency
                    block_timestamp = event.get('block_timestamp')
                    if block_timestamp:
                        now_ts = int(time.time())
                        latency_sec = now_ts - int(block_timestamp)
                        logger.info(f"Latency: {latency_sec}s")
    
    def _execute_jup_perps_event(self, works):
        start_block = works[0]
        end_block = works[-1]
        events_cursor = self.importer.get_solana_jup_perps_events(
            start_block=start_block, end_block=end_block,
        )
        events = list(events_cursor)

        for event_type, signal_type in self.jup_perps_event_types.items():
            events_by_type = [event for event in events if event['event_type'] == event_type]

            if (not events_by_type):
                continue

            logger.info(f'Execute jup perps event {signal_type} with {len(events_by_type)} events')

            for event in events_by_type:
                THRESHOLD = JUP_PERPS_THRESHOLD.get(signal_type.lower())
                self.executor.jup_perps_event_information(event, klg_db=self.klg_db)

                if event.get('threshold') > THRESHOLD:
                    signal = {
                        'id': event['_id'],
                        'chainId': self.chain_id,
                        'timestamp': event['block_timestamp'],
                        'blockNumber': event['block_number'],
                        'transactionHash': event['transaction_hash'],
                        'wallet': event['wallet'],
                        "explorerUrl": f"{self.chain_scan}tx/{event['transaction_hash']}",
                        'signalType': signal_type,
                        'valueType': event.get('positionType'),
                        'assets': event.get('assets'),
                        'tags': [event.get('positionType')],
                        'projectId': event.get('project').get('projectId'),
                        'projectName': event.get('project').get('projectName'),
                        'projectImgUrl': event.get('project').get('projectImgUrl'),
                        'projectType': event.get('project').get('projectType'),
                        'description': "Has been applied to Lee Quid",
                    }

                    value = event.get('sizeUsdDelta') if event.get('sizeUsdDelta') else event.get('pnlDelta')
                    value = float(value)/1e6
                    signal['value'] = f"${value:,.2f}"

                    if signal_type == "open_position":
                        signal['extraInfo'] = {
                            'owner': event.get('owner'),
                            'feeUsd': float(event.get('feeUsd'))/1e6,
                            'leverage': event.get('leverage'),
                            'positionType': event.get('positionType'),
                            'collateralUsdDelta': float(event.get('collateralUsdDelta'))/1e6,
                        }

                    elif signal_type == "close_position":
                        signal['extraInfo'] = {
                            'owner': event.get('owner'),
                            'feeUsd': float(event.get('feeUsd'))/1e6,
                            'leverage': event.get('leverage'),
                            'hasProfit': event.get('hasProfit'),
                            'pnl': float(event.get('pnlDelta'))/1e6 if event.get('hasProfit') == "True" else (-1)*float(event.get('pnlDelta'))/1e6,
                            'positionType': event.get('positionType'),
                        }

                    elif signal_type == "liquidate":
                        signal['extraInfo'] = {
                            'owner': event.get('owner'),
                            'feeUsd': float(event.get('feeUsd'))/1e6,
                            'liquidationFeeUsd': float(event.get('liquidationFeeUsd'))/1e6,
                            'hasProfit': event.get('hasProfit'),
                            'pnl': float(event.get('pnlDelta'))/1e6 if event.get('hasProfit') == "True" else (-1)*float(event.get('pnlDelta'))/1e6,
                            'positionType': event.get('positionType'),
                        }

                    self._signals.append(signal)

                    # log signal latency
                    block_timestamp = event.get('block_timestamp')
                    if block_timestamp:
                        now_ts = int(time.time())
                        latency_sec = now_ts - int(block_timestamp)
                        logger.info(f"Latency: {latency_sec}s")
                
    def _execute_batch(self, works):
        if "liquidity-pool-events-collector" in self.collector_id:
            self._execute_dex_event(works)
            self._execute_swap_event(works)

        if "lending_events" in self.collector_id:
            self._execute_lending_event(works)

        if "jup-perps-events" in self.collector_id:
            self._execute_jup_perps_event(works)

    def _export(self):
        # Notify signals
        self.exporter.export_items(self._signals)
        logger.info(f'Exported {len(self._signals)} signals')
