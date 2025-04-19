import json
from typing import Any, Dict, List, Optional
import time
import redis

from src.constants.blockchain_etl import DBPrefix
from src.constants.config import RedisConfig
from src.constants.llm.agent_prompt import SignalExplanationPromptTemplate
from src.constants.network import Chains
from src.constants.signal import SignalStream
from src.databases.mongodb_community import MongoDBCommunity
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger

logger = get_logger("Signal Explainer")


class SignalExplainer:
    def __init__(self):
        self.mongodb_community = MongoDBCommunity()
        self.llm = LLMCommunication()
        self.redis_client = redis.from_url(
            RedisConfig.CONNECTION_URL, decode_responses=True
        )

    def check_signal_by_filter(
        self,
        signal: Dict[str, Any],
        chain_id: Optional[str] = None,
        signal_type: Optional[str] = None,
    ) -> bool:
        """Filter signals based on chain_id and signal_type"""
        if chain_id and str(signal.get("chainId")) != chain_id:
            return False
        if signal_type and signal.get("type") != signal_type:
            return False
        return True

    def get_signals(
        self,
        n_signals: int = 10,
        chain_id: Optional[str] = None,
        signal_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        signals = {}
        last_id = "+"

        messages = self.redis_client.xrevrange(
            SignalStream.stream_name, max=last_id, count=n_signals
        )

        for entry_id, message in messages:
            signal = json.loads(message["msg"])
            if not self.check_signal_by_filter(
                signal, chain_id=chain_id, signal_type=signal_type
            ):
                continue
            signals[signal.get("_id", signal.get("id"))] = signal

        signals = {
            k: v
            for k, v in signals.items()
            if (v.get("signalType") != "highest_apy")
        }

        signals = {
            k: v
            for k, v in signals.items()
            if v.get("timestamp") > int(time.time()) - 1200
        }

        signal_list = list(signals.values())
        signal_list.sort(key=lambda x: x["blockNumber"])

        return signal_list

    def explain_signal(self, signal: Dict[str, Any]) -> str:
        signal_explanation_prompt_template = SignalExplanationPromptTemplate()
        template = signal_explanation_prompt_template.create_template()

        prompt = template.format(
            timestamp=signal.get("timestamp"),
            signal_type=signal.get("signalType"),
            value=signal.get("value"),
            chain_name=DBPrefix.mapping[Chains.names[signal.get("chainId")]],
            tokens=signal.get("assets"),
            project=signal.get("projectName"),
            contract_address=signal.get("contractAddress"),
            wallet_address=signal.get("walletAddress"),
        )

        response = self.llm.send_prompt(prompt)
        return response

    def get_and_explain_signals(
        self,
        n_signals: int = 100,
        chain_id: Optional[str] = None,
        signal_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get signals and add explanations"""
        signals = self.get_signals(n_signals, chain_id, signal_type)

        explained_signals = self.mongodb_community._db["defi_signals"].find(
            {
                "isExplained": {"$exists": True},
                "_id": {"$in": [signal.get("_id", signal.get("id")) for signal in signals]},
            },
        )
        explained_signals_ids = {signal["_id"] for signal in explained_signals}
        signals = [
            signal
            for signal in signals
            if signal.get("_id", signal.get("id")) not in explained_signals_ids
        ]

        for signal in signals:
            explanation = self.explain_signal(signal)
            signal["_id"] = signal.get("_id", signal.get("id"))
            signal["explanation"] = explanation
            signal["isExplained"] = True

        if len(signals) > 0:
            self.mongodb_community.update_docs(collection_name="defi_signals", data=signals)
            logger.info(f"Explained {len(signals)} signals")
