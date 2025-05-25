from abc import ABC, abstractmethod
from typing import Any

from langchain.prompts import PromptTemplate
from pydantic import BaseModel


class BasePromptTemplate(ABC, BaseModel):
    @abstractmethod
    def create_template(self, *args: Any) -> str:
        pass


class NewsPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As an Influencer in the Web3 Community, generate a creative, concise tweet based solely on the provided information (keyword, description, and summary). [/INST]
        Response must meet the following criteria:
            - Break the line for each sentence related to a summary.
            - Must mention token/coin with capitalize characters and $ appear first if applicable (For examples: $LINK). 
            - Limit to under 280 characters, avoiding summaries' tone or engagement/promotion/question phrases in the last sentence. 
            - Exclude emojis, hashtags, and icons. 

        Example of a good response:
            $BTC faces potential market shift.  
            U.S. approved sale of 198,000 BTC from Silk Road, valued at $6.5 billion.  
            Bitcoin dips below $92,000 on oversupply fears.
            

        Here is your information:
        - Keyword: {information_keyword}
        - Description: {information_description}
        - Summary: {information_summary}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=[
                "information_keyword",
                "information_description",
                "information_summary",
            ],
        )


class ReplyNewsPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful agent that is an Influencer in Web3 Community. Your task is to generate a reply tweet
        based on the tweet related to a keyword.
        I will provide you with tweet related to a keyword; list of tokens affected by the keyword; summary of articles related to tokens respectively; its sentiment respectively.
        With each token, you need to generate a reply tweet. [/INST]

        Your response must meet the following criteria:
        - It must be a list of replys, no additional information. Format of reply: [reply1, reply2, reply3, ...]
        - It must be creative and not should not follow the tone of summary.
        - Each reply must mention token with the characters $ appear fisrt (for example: $LINK) if it contains token.
        - Each reply must be under 280 characters
        - The reply should be very shorten and immediately focus on the main idea.
        - Do not contain the promotion or engagement content in the last sentence of the reply (Like: Keep an eye on.. ,Stay up to date).
        - Do not contain emojis, hashtags, icons.

        Here is your information:
        - Keyword: {information_keyword}
        - Tweet: {information_tweet}
        - Tokens Affected: {information_tokens_affected}
        - Summary: {information_summary}
        - Sentiment: {information_sentiment}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=[
                "information_keyword",
                "information_tweet",
                "information_tokens_affected",
                "information_summary",
                "information_sentiment",
            ],
        )


class AnalysisPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] You are a helpful agent specialized in analyzing the Web3 market.
        Your task is to generate an Up-To-Date, concise analysis report based on the provided information, focusing specifically on the most notable changes in the latest days.

        You will receive:
        - Entity name
        - Timeseries data of the entity
        - Tag of the entity (token or project)

        Interpretation Guide:
        - If tag = token: Analyze both Price and MarketCap trends.
        - If tag = project: Analyze the TVL trend.

        Your analysis must:
        - Paraphrase the information in only 3-4 sentences, no more than 280 characters.
        - No definitions or introductions. Jump straight into the analysis.
        - Focus ONLY on significant recent movements (sharp rises or drops).
        - Prioritize mentioning the percentage change over absolute numbers, unless the price/TVL/marketcap level is exceptionally notable.
        - Highlight if price, marketcap, or TVL hits new highs/lows recently.
        - Use a free-format style suited for a Twitter post.
        - Mentioning token names with $ and fully capitalized (e.g., $LINK).
        - Maintain an informative, summarizing tone.
        - DO NOT add promotional/engagement/introductory phrases (such as "stay tuned", "keep an eye").
        - Do NOT use the same tone or wording structure every time.
        - Exclude emojis, hashtags, and icons. 

        Here is your information:
        - Entity: {entity}
        - TimeseriesData: {timeseries_data}
        - Tag: {tag}
        [/INST]
    """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["entity", "timeseries_data", "tag"],
        )


class EntityExtractionPromptTemplate(BasePromptTemplate):
    prompt: str = """
        Extract all entities that belong to the list below from the given text.
        Your response must be in dictionary format with the key is the word and the value is the entity type. 
        Do not miss any entity in the text, include abbreviation; and only include entities belong to the list below.

        *** List of entity types: [
            "Token Cryptocurrency", "Lending", "RWA Lending", "NFT Lending", "Staking",
            "Liquid Staking", "Restaking", "Yield", "Yield Aggregator", "Leveraged Farming",
            "Indexes", "Synthetics", "Derivatives", "Prediction Market", "NFT Marketplace",
            "Launchpad", "Chain", "Bridge", "Cross Chain", "DEX Aggregator",
            "Wallet Address", "Telegram Bot", "Basis Trading", "Liquidations",
            "DeFi Project", "RWA", "Concept", "Technology"
        ]

        *** Text: {text}
        """
    
    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["text"],
        )
    

class FindLinkOfEntity(BasePromptTemplate):
    prompt: str = """
        You are a blockchain knowledge assistant.

        Given a list of entities, return a dictionary where:
        - the key is the entity name
        - the value is a relevant knowledge link that helps users learn more about that entity.

        Return only one best link per entity and that link must be available now.

        Format: JSON dictionary.

        Entities:
        {entities}
    """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["entities"]
        )
    

class SignalExplanationPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a DeFi analytics expert explaining blockchain signals, your task is to explain the given DeFi signal in detail.
        I will provide you with signal type, value of the signal, chain name, tokens related to the signal, project related to the signal.
        I have some signal type that are described below:
            - add_liquidity: Add liquidity to a pool.
            - remove_liquidity: Remove liquidity from a pool.
            - temporarily_exit: User temporarily removed liquidity from a pool and added it back after a while.
            - new_listing: New pair listed on DEX.
            - deposit: Deposit lending event.
            - withdraw: Withdraw lending event.
            - borrow: Borrow lending event.
            - repay: Repay lending event.
            - liquidate: Liquidate event.
            - open_position: Open position in jupiter perpetual.
            - close_position: Close position in jupiter perpetual.
            - swap: Swap event of whale.

        Important rules:
            - Do NOT use the same tone or wording structure every time. Each explanation must feel fresh, with a different style or personality.
            - Be creative with phrasing, sentence rhythm, and expression.
            - The explanation should feel natural, human-like, short, and organic.
            - No definitions or introductions. Jump straight into the explanation.
            - No impact analysis, no intro, no context, no explanation of terms—just say what the signal shows.
            - No need exclamation or excitement.
     
        Here is your information:
        - Signal Type: {signal_type}
        - Value: {value}
        - Chain name: {chain_name}
        - Tokens: {tokens}
        - Project: {project}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["signal_type", "chain_id", "block_number", "signal_data"],
        )