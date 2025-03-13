import bisect

from sqlalchemy import (
    BigInteger,
    Column,
    Float,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    inspect,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from src.constants.config import PostgreRAGConfig, PostgresDBETLConfig
from src.utils.list_dict import check_dict_have_null_values
from src.utils.logger import get_logger

logger = get_logger("PostgreSQL thread safe")

Session = sessionmaker()
Base = declarative_base()


# def TokenTransferTable(chain_id):
#     class TokenTransfer(Base):
#         __tablename__ = f"agents_token_transfers_{chain_id}"
#         __table_args__ = {"extend_existing": True}

#         # id = Column(Integer, primary_key=True, autoincrement=True)
#         contract_address = Column(String(), nullable=False)
#         block_number = Column(BigInteger(), nullable=False)
#         from_address = Column(String(), nullable=False)
#         to_address = Column(String(), nullable=False)
#         value = Column(
#             Float(5),
#             nullable=False,
#         )
#         value_in_usd = Column(
#             Float(5),
#             nullable=False,
#         )

#     return TokenTransfer


class PostgresDB:
    def __init__(self, chain_id, connection_url: str = None, rag=False):
        # Set up the database connection and create the table
        if not connection_url:
            if rag is False:
                connection_url = PostgresDBETLConfig.CONNECTION_URL
            elif rag is True:
                connection_url = PostgreRAGConfig.CONNECTION_URL

        Session.configure(bind=create_engine(connection_url))
        self.chain_id = chain_id
        self.engine = create_engine(connection_url)

    def query_transfer_events(
        self,
        contract_addresses,
        from_block,
        to_block,
        block_timestamp_mappings,
        price_history,
    ):
        token_lst = [token for token, _ in price_history.items()]

        if not self.chain_id:
            schema = PostgresDBETLConfig.SCHEMA
        else:
            schema = f"chain_{self.chain_id}"
        addresses_values = ", ".join([f"'{cls}'" for cls in contract_addresses])
        query = f"""
            SELECT contract_address, log_index, block_number, from_address, to_address, value
            FROM {schema}.{PostgresDBETLConfig.TRANSFER_EVENT_TABLE}
            WHERE contract_address IN ({addresses_values}) 
            AND block_number BETWEEN {from_block} AND {to_block}

        """

        wallet_transfer = []

        with Session.begin() as session:
            event_transfer = session.execute(text(query))
            fields_to_select = [
                "contract_address",
                "log_index",
                "block_number",
                "from_address",
                "to_address",
                "value",
            ]
            for event in event_transfer:
                value_in_usd = self._get_valueInUSD(
                    row=event,
                    token_history=price_history,
                    token_lst=token_lst,
                    block_timestamp_mappings=block_timestamp_mappings,
                )

                modified_event = (
                    event[:2] + (block_timestamp_mappings[event[2]],) + event[3:]
                )
                timestamp_event = dict(zip(fields_to_select, modified_event))
                timestamp_event["value_in_usd"] = value_in_usd
                wallet_transfer.append(timestamp_event)

        return wallet_transfer

    def query_agents_transfer_events(
        self,
        contract_addresses,
        from_block,
        to_block,
        block_timestamp_mappings,
        price_history,
        special_wallets,
    ):
        token_lst = [token for token, _ in price_history.items()]

        if not self.chain_id:
            schema = PostgresDBETLConfig.SCHEMA
        else:
            schema = f"chain_{self.chain_id}"
        addresses_values = ", ".join([f"'{cls}'" for cls in contract_addresses])
        special_wallet_addresses = ", ".join([f"'{cls}'" for cls in special_wallets])

        query = f"""
            WITH TypeA AS (
                SELECT contract_address, block_number, from_address, to_address, value
                FROM {schema}.{PostgresDBETLConfig.TRANSFER_EVENT_TABLE}
                WHERE contract_address IN ({addresses_values}) 
                AND block_number BETWEEN {from_block} AND {to_block}

            ),
            TypeB AS (
                SELECT contract_address, block_number, from_address, to_address, value
                FROM {schema}.{PostgresDBETLConfig.TRANSFER_EVENT_TABLE}
                WHERE from_address IN (SELECT from_address FROM TypeA)
                OR from_address IN (SELECT to_address FROM TypeA)
                OR to_address IN (SELECT from_address FROM TypeA)
                OR to_address IN (SELECT to_address FROM TypeA)
                AND from_address NOT IN ({special_wallet_addresses})
                AND to_address NOT IN ({special_wallet_addresses})   
                
            )
            SELECT contract_address, block_number, from_address, to_address, value FROM TypeA
            UNION ALL
            SELECT contract_address, block_number, from_address, to_address, value FROM TypeB
            WHERE block_number BETWEEN {from_block} AND {to_block}
;


        """

        wallet_transfer = []
        with Session.begin() as session:
            event_transfer = session.execute(text(query))
            fields_to_select = [
                "contract_address",
                "log_index",
                "block_number",
                "from_address",
                "to_address",
                "value",
            ]

            for event in event_transfer:
                value_in_usd = self._get_valueInUSD(
                    row=event,
                    token_history=price_history,
                    token_lst=token_lst,
                    block_timestamp_mappings=block_timestamp_mappings,
                )
                modified_event = (
                    event[:1] + (block_timestamp_mappings[event[1]],) + event[2:]
                )
                timestamp_event = dict(zip(fields_to_select, modified_event))
                timestamp_event["value_in_usd"] = value_in_usd
                if check_dict_have_null_values(timestamp_event):
                    wallet_transfer.append(timestamp_event)

        return wallet_transfer

    def create_table(self):
        if not self.table_exists(table_name=f"agent_token_transfers_{self.chain_id}"):
            CREATE_TABLE_QUERY = f"""
                CREATE TABLE agent_token_transfers_{self.chain_id}
                    (
                        contract_address text COLLATE pg_catalog."default",
                        log_index bigint,
                        block_number bigint,
                        from_address text COLLATE pg_catalog."default",
                        to_address text COLLATE pg_catalog."default",
                        value double precision,
                        value_in_usd double precision,
                        CONSTRAINT agent_token_transfers_{self.chain_id}_block_number_log_index_key UNIQUE (log_index, block_number)
                    )
                TABLESPACE pg_default;

                CREATE INDEX IF NOT EXISTS agent_token_transfers_{self.chain_id}_block_number_index
                ON agent_token_transfers_{self.chain_id} USING btree
                (block_number ASC NULLS LAST)
                TABLESPACE pg_default;

                CREATE INDEX IF NOT EXISTS agent_token_transfers_{self.chain_id}_to_address_index
                ON agent_token_transfers_{self.chain_id} USING btree
                (to_address COLLATE pg_catalog."default" ASC NULLS LAST)
                TABLESPACE pg_default;

                CREATE INDEX IF NOT EXISTS agent_token_transfers_{self.chain_id}_from_address_index
                ON agent_token_transfers_{self.chain_id} USING btree
                (from_address COLLATE pg_catalog."default" ASC NULLS LAST)
                TABLESPACE pg_default;

                CREATE INDEX IF NOT EXISTS agent_token_transfers_{self.chain_id}_contract_address_index
                ON agent_token_transfers_{self.chain_id} USING btree
                (contract_address COLLATE pg_catalog."default" ASC NULLS LAST)
                TABLESPACE pg_default;


            """

            with Session.begin() as session:
                # stmt = insert(table).values(data)
                session.execute(CREATE_TABLE_QUERY)
                session.commit()

            print(f"Successful create table: agents_token_transfers_{self.chain_id}")

        else:
            print(f"Table agents_token_transfers_{self.chain_id} have already created")

    def insert_transfer_data_to_table(self, data):
        # insert_query = f"""
        # INSERT INTO agents_token_transfers_{self.chain_id} (contract_address, log_index, block_number, from_address, to_address, value, value_in_usd) VALUES (%s, %s, %s, %s, %s, %s, %s)
        # """
        # print(insert_query)

        # with Session.begin() as session:
        #     session.execute(insert_query, data)
        #     session.commit()
        metadata = MetaData()
        items = Table(
            f"agent_token_transfers_{self.chain_id}",
            metadata,
            Column("contract_address", String),
            Column("log_index", BigInteger),
            Column("block_number", BigInteger),
            Column("from_address", String),
            Column("to_address", String),
            Column("value", Float(5)),
            Column("value_in_usd", Float(5)),
        )

        with self.engine.begin() as connection:
            connection.execute(insert(items), data)

    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def _largest_key_smaller_than(self, dictionary, value):
        keys = sorted(dictionary.keys())
        index = bisect.bisect_left(keys, value) - 1
        if index >= 0:
            return keys[index]
        else:
            return 0

    def _get_valueInUSD(self, row, token_history, token_lst, block_timestamp_mappings):
        token = row[0]  # contract
        if token in token_lst:
            if token_history[token] != {}:
                timestamp = row[2]
                round_timestamp = (
                    block_timestamp_mappings[timestamp]
                    - block_timestamp_mappings[timestamp] % 86400
                ) * 1000  # change to timestamp daily, not times 1000
                if f"{round_timestamp}" in list(token_history[token].keys()):
                    value_in_usd = (
                        (row[-1]) * token_history[token][f"{round_timestamp}"]
                    )
                else:
                    largest_key = self._largest_key_smaller_than(
                        token_history[token], str(round_timestamp)
                    )
                    if largest_key == 0:
                        value_in_usd = 0
                    else:
                        last_value = token_history[token][largest_key]
                        value_in_usd = (row[-1]) * last_value
                return value_in_usd
            else:
                return None
        else:
            return None
