from typing import List


class LiquidityPool:
    def __init__(self, address, chain_id):
        self.address = address
        self.chain_id = chain_id
        self.tokens = []
        self.liquidity_amount = 0
        self.decimals = 18
        self.last_interacted_at = 0
        self.last_check_reserves_at = 0
        self.project = None
        self.last_interacted_at_block = 0

        self.metadata = {}

    @classmethod
    def from_dict(cls, json_dict: dict):
        address = json_dict['address']
        chain_id = json_dict['chainId']

        self = cls(address=address, chain_id=chain_id)
        self.tokens = json_dict.get('tokens', [])
        self.last_interacted_at = json_dict.get('lastInteractedAt', 0)
        self.last_check_reserves_at = json_dict.get('lastCheckReservesAt', 0)
        self.liquidity_amount = json_dict.get('liquidityAmount', 0)
        self.decimals = json_dict.get('decimals', 18)
        self.project = json_dict.get("project")

        return self

    def to_dict(self):
        data = {
            'address': self.address,
            'chainId': self.chain_id,
            'tokens': self.tokens,
            'project': self.project,
            'lastInteractedAt': self.last_interacted_at,
            'lastCheckReservesAt': self.last_check_reserves_at,
            **{k: v for k, v in self.metadata.items() if v is not None}
        }
        if self.liquidity_amount > 0:
            data.update({'liquidityAmount': self.liquidity_amount, 'decimals': self.decimals})

        return data

    def check_missed_information(self, required_fields: List[str] = None, required_metadata_fields: List[str] = None, number_of_tokens: int = None):
        if required_fields is not None:
            missed_fields = [field for field in required_fields if self.__getattribute__(field) is None]
            if missed_fields:
                return True

        if required_metadata_fields is not None:
            missed_metadata_fields = [field for field in required_metadata_fields if self.metadata.get(field) is None]
            if missed_metadata_fields:
                return True

        if (not self.tokens) or ((number_of_tokens is not None) and (len(self.tokens) != number_of_tokens)):
            return True

        for token in self.tokens:
            if token.get('decimals') is None:
                return True

        return False
