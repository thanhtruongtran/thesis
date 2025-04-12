from src.utils.logger import get_logger

logger = get_logger('Protocol mapping')


def get_protocol_address_mapping(protocols: dict):
    """
    Return mapping:
    - Aave V2, V3 Fork: lending_pool_address -> protocol_id
    - Compound Fork: c_token_address -> protocol_id
    - Compound V3 Fork: comet_address -> protocol_id
    - Morpho Fork: comptroller_address -> protocol_id
    - Silo Fork: pool_address -> protocol_id
    """
    mapping = {}
    for protocol_id, info in protocols.items():
        mapping.update(get_protocol_mapping(pool_info=info, value=protocol_id))

    return mapping


def get_protocol_mapping(pool_info, value, chain_id=None):
    mapping = {}

    forked_from = pool_info.get('forked')
    if not forked_from:
        return {}

    elif forked_from == 'compound':
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['cToken'].lower()
            mapping[address] = value

    elif forked_from.startswith('aave'):
        address = pool_info['address'].lower()
        mapping[address] = value
    elif forked_from == 'compound-v3':
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['comet'].lower()
            mapping[address] = value
    elif forked_from.startswith('morpho'):
        address = pool_info['comptrollerAddress'].lower()
        mapping[address] = value
    elif forked_from.startswith('silo'):
        reserves_list = pool_info.get('reservesList', {})
        for market_info in reserves_list.values():
            address = market_info['pool'].lower()
            mapping[address] = value
    elif pool_info.get('address'):
        logger.warning(f'Missing handle fork {forked_from}')
        address = pool_info['address'].lower()
        mapping[address] = value

    if chain_id is not None:
        mapping = {f'{chain_id}_{address}': v for address, v in mapping.items()}

    return mapping


def get_compound_mapping_asset(pool_info, chain_id=None):
    mapping = {}

    forked_from = pool_info.get('forked')
    if not forked_from:
        return {}

    elif forked_from == 'compound':
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            address = market_info['cToken'].lower()
            mapping[address] = asset_address.lower()

    elif forked_from == 'compound-v3':
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            address = market_info['comet'].lower()
            mapping[address] = asset_address.lower()
    elif forked_from.startswith('silo'):
        reserves_list = pool_info.get('reservesList', {})
        for asset_address, market_info in reserves_list.items():
            address = market_info['pool'].lower()
            mapping[address] = asset_address.lower()

    if chain_id is not None:
        mapping = {f'{chain_id}_{address}': v for address, v in mapping.items()}

    return mapping


def mapping_protocol_by_event(pool_info, event):
    contract_address = event.get('contract_address')
    collateral_asset = event.get('collateral_asset')
    debt_asset = event.get('debt_asset')
    reserve = event.get('reserve')
    if not contract_address:
        return None

    forked_from = pool_info.get('forked')
    if not forked_from:
        return None

    reserves_list = pool_info.get('reservesList', {})
    token_address = None
    collateral_asset_address = None
    debt_asset_address = None

    result = {
        "projectId": pool_info.get('_id', '').split('_')[1],
        "projectName": pool_info.get("name"),
        "contractAddress": event.get('contract_address'),
        "forkedFrom": forked_from,
    }

    if event.get('event_type') == 'LIQUIDATE':
        if collateral_asset and debt_asset:
            collateral_asset_address = collateral_asset
            debt_asset_address = debt_asset
        elif collateral_asset:
            collateral_asset_address = collateral_asset
            if forked_from == 'compound':
                for asset_address, market_info in reserves_list.items():
                    address = market_info.get('cToken', '').lower()
                    if contract_address == address:
                        debt_asset_address = asset_address
                        break

            elif forked_from.startswith('compound-v3'):
                for asset_address, market_info in reserves_list.items():
                    address = market_info.get('comet', '').lower()
                    if contract_address == address:
                        debt_asset_address = asset_address
                        break

            elif forked_from.startswith('silo'):
                for asset_address, market_info in reserves_list.items():
                    address = market_info.get('pool', '').lower()
                    if contract_address == address:
                        debt_asset_address = asset_address
                        break
        
        result['collateralAssetAddress'] = collateral_asset_address
        result['debtAssetAddress'] = debt_asset_address

    else:
        if forked_from == 'compound':
            for asset_address, market_info in reserves_list.items():
                address = market_info.get('cToken', '').lower()
                if contract_address == address:
                    token_address = asset_address
                    break
            
        elif forked_from.startswith('aave'):
            address = pool_info.get('address', '').lower()
            if contract_address == address:
                token_address = reserve

        elif forked_from == 'compound-v3':
            for asset_address, market_info in reserves_list.items():
                address = market_info.get('comet', '').lower()
                if contract_address == address:
                    token_address = asset_address
                    break

        elif forked_from.startswith('morpho'):
            address = pool_info.get('comptrollerAddress', '').lower()
            if contract_address == address:
                token_address = reserve
                
        elif forked_from.startswith('silo'):
            for asset_address, market_info in reserves_list.items():
                address = market_info.get('pool', '').lower()
                if contract_address == address:
                    token_address = asset_address
                    break

        result['tokenAddress'] = token_address
        
    has_token = result.get("tokenAddress")
    has_collateral_and_debt = result.get("collateralAssetAddress") and result.get("debtAssetAddress")
    if not has_token and not has_collateral_and_debt:
        return None
    
    return result
