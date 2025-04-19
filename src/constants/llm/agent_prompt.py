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


class RecentAnalysisPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful agent that is an Analyst in Web3 Community. Your task is to generate an Up To Date analysis report based on my provided information such as: token, the timeseries data of that token (For example: TVL (Total value locked), MarketCap, Prices or their users like number of Active Users, Holders) [/INST]

        Your response must meet the following criteria:
        - Using free formating style to start the Twitter post.
        - It must be under 220 characters.
        - The analysis MUST ONLY focus on the trend of lattest days where happening significant rise or drop.
        - Prefer to show the percentage rising or falling instead of specific price, only take price when it is so significant.
        - It must be informative and follow the tone of summary.
        - It must mention token with capitalize characters and $ appear first if applicable (For examples: $LINK).
        - Do not contains emoji, hashtags.
        - The post should be very shorten and should break the line for each idea (Go down two lines).
        - One line NOT exceed 2 sentences.
        - NOT include the promotion or engagement content in the last sentence of the post (Like: Keep an eye on.. ,Stay up to date).
        - Using paraphrase word of significant.


        Here is your information:
        - Token: {analysis_token}
        - TimeseriesData: {timeseries_data}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["analysis_token", "timeseries_data"],
        )


class PeriodAnalysisPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful agent that is an Analyst in Web3 Community. Your task is to generate an summarize Period analysis report based on my provided information such as: token, the timeseries data of that token (For example: TVL (Total value locked), MarketCap, Prices or their users like number of Active Users, Holders and Anomal points - The points that value change significantly for these features). [/INST]

        Your response must meet the following criteria:
        - Using free formating style to start the Twitter post, it must be posted in Twitter with no more than 220 characters.
        - The analysis is an very short analyze report on the overall trend whole period, focus on the lattest days, only talk about the main idea and main number.
        - Prefer to show the percentage rising or falling instead of specific price, only take price when it is so significant.
        - It must mention token with capitalize characters and $ appear first if applicable (For examples: $LINK).
        - Do not contains hashtags, emojis
        - You MUST devide different ideas into replies (Maximum 1-2 replies) by only character //, the first letter must be capitalized.
        - The post should be very shorten and should break the line for each idea (Go down two line).
        - One line NOT exceed 2 sentences.
        - NOT include the promotion or engagement content in the last sentence of the post (Like: Keep an eye on.. ,Stay up to date).
        - Using paraphrase word of significant.


        Here is your information:
        - Token: {analysis_token}
        - AnomalPoints: {anomal_points}
        - TimeseriesData: {timeseries_data}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["analysis_token", "anomal_points", "timeseries_data"],
        )


class TuningReplyPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful assistant in Twitter, your task is tuning the reply tweet to make it more natural like human.
        I will provide you with the original post, user's reply tweet and my initial reply tweet. [/INST]

        Your response must meet the following criteria:
        - It must be a tweet, no additional information.
        - It should be very very natural, human-like, not robotic.
        - If user's reply tweet is too short and meaningless, generate a new reply tweet with joke, humor or meme tone.
        - If my initial reply tweet is too detailed, tune it to a very short and relevant tweet.
        - It must be short with a tone of reply, not a post.
        - Do not contains emoji, hashtags.

        Here is your information:
        - Original Post: {original_tweet}
        - User's Reply Tweet: {reply_tweet}
        - My Initial Reply Tweet: {initial_reply_tweet}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["original_tweet", "reply_tweet", "initial_reply_tweet"],
        )


class TrendingMentionedTokenPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful agent that is an Analyst in Web3 Community. Your task is to generate an analysis or news Twitter post based on the given information about recent hot crypto tokens.
        I will provide you symbol, recent tweets related to the token and timeseries data. 

        Your response must meet the following criteria:
        - Do not use the timeseries data in MOST cases, unless if there are all unuseful tweets information (For example: Do not have any analysis, news information), then use the Timeseries data and skip all that tweets.
        - It must be posted in Twitter with no more than 260 characters.
        - Prefer to show the PERCENTAGE rising or falling instead of specific number, only take price when it is so significant.
        - It must mention token with capitalize characters and $ appear first if applicable (For examples: $LINK).
        - You MUST devide different ideas into replies (Maximum 1-2 replies) by only character //, the first letter must be capitalized.
        - NOT include the promotion or engagement content in the last sentence of the post (Like: Keep an eye on.. ,Stay up to date).
        - Do not contains hashtags, emojis.
        - The post must follow a format where each single idea, sentence is BROKEN into a new line (Go down two line), like below example.

        Follow this format:
            No more cap, I told you to buy $BUZZ.

            You could make 20x - 30x from my calls.
            
            Now it's time for $ORAC (bro can literally predict future).

            - FULL opensource github code
            - AI Agent that helps and predicts the future


        Here is your information:
        - Symbol: {symbol}
        - Tweets: {tweets}
        - Timeseries data: {timeseries_data}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["symbol", "tweets", "timeseries_data"],
        )


class KeywordMentionedPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful agent that is an Analyst in Web3 Community. Your task is to generate an analysis or news Twitter post based on the given information about recent hot crypto keyword.
        I will provide you keyword, recent tweets related to the keyword. 

        Your response must meet the following criteria:

        - It must be posted in Twitter with no more than 260 characters.
        - The writting style should be in the formal written.
        - Prefer to show the PERCENTAGE rising or falling instead of specific number, only take price when it is so significant.
        - It must mention token with capitalize characters and $ appear first if applicable (For examples: $LINK).
        - You MUST devide different ideas into replies if content is too long which is more than 260 characters (Maximum 1-2 replies) by only character //, the first letter must be capitalized.
        - NOT include the promotion or engagement content in the last sentence of the post (Like: Keep an eye on.. ,Stay up to date).
        - Do not contains hashtags, emojis.
        - The post must follow a format where each single idea, sentence is BROKEN into a new line (Go down two line), like below example.

        Follow this format:
            $MLG down to 15m after LA vape cabal pump

            That's -90% from ath in just 1.5 weeks. Adin eating an 85% loss on his position
            Same wallet that bought 10m of jailstool was in $MLG and $TRUMP before

            
        Here is your information:
        - Keyword: {keyword}
        - Tweets: {tweets}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["keyword", "tweets"],
        )


class TokenNewsPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful agent that is an Analyst in Web3 Community. Your task is to generate an analysis or news Twitter post based on the given information about recent hot crypto tokens.
        I will provide you symbol, list of recent news related to the token. 

        Your response must meet the following criteria:
            - Break the line for each sentence related to a summary.
            - Must mention token/coin with capitalize characters and $ appear first if applicable (For examples: $LINK). 
            - Limit to under 280 characters, avoiding summaries' tone or engagement/promotion/question phrases in the last sentence. 
            - Exclude emojis, hashtags, and icons. 

        Follow this format:
            $BTC faces potential market shift.  
            U.S. approved sale of 198,000 BTC from Silk Road, valued at $6.5 billion.  
            Bitcoin dips below $92,000 on oversupply fears.

        Here is your information:
        - Symbol: {symbol}
        - News: {news}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["symbol", "news"],
        )


class ReFormatPromptTemplate(BasePromptTemplate):
    prompt: str = """
        <s>[INST] As a helpful assistant in Twitter, your task is to reformat the tweet to make it more natural like human.
        I will provide you with the original post. [/INST]

        Your response must meet the following criteria:
        - If original post has multiple sentences or ideas, must break the line for each sentence or idea.
        - It should be very very natural, human-like, not robotic.
        - Do not contains emoji, hashtags.
        - Must not start with '@' character, but must use it to mention the project's username in the middle of the post.

        An example of a good response:
            $BTC faces potential market shift. 
            U.S. approved sale of 198,000 BTC from Silk Road, valued at $6.5 billion. 
            Bitcoin dips below $92,000 on oversupply fears.

        Here is your information:
        - Original Post: {original_tweet}
        """

    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["original_tweet"],
        )


class EntityExtractionPromptTemplate(BasePromptTemplate):
    prompt: str = """
        You are an expert in Web3 Community and NLP. Your task is to extract entities from the given text.

        Entities to extract: ["TOKEN", "PROJECT", "WALLET", "EVENT", "TREND"]

        Your response MUST be in dictionary format with the key is the entity and the value is a list of entities.
        Do not include symbol, puctuation, ```, or any special characters in the extracted entities.

        For example:
            "TOKEN": ["$BTC", "$ETH"],
            "PROJECT": ["Bitcoin", "Ethereum"],
            "WALLET": ["MetaMask", "Trust Wallet"],
            "EVENT": ["Hackathon", "Conference"],
            "TREND": ["DeFi", "NFT"]

        Text: {text}
        """
    
    def create_template(self) -> PromptTemplate:
        return PromptTemplate(
            template=self.prompt,
            input_variables=["text"],
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