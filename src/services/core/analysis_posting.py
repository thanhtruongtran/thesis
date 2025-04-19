import pandas as pd

from src.constants.llm.agent_prompt import (
    KeywordMentionedPromptTemplate,
    PeriodAnalysisPromptTemplate,
    RecentAnalysisPromptTemplate,
    TrendingMentionedTokenPromptTemplate,
)
from src.constants.text_processing import TwitterTweetConstant
from src.services.llm.communication import LLMCommunication
from src.utils.time import change_timestamp


class AnalysisPostingService:
    def __init__(self, token_info=None, symbol=None):
        self.llm = LLMCommunication()
        self.token_info = token_info
        self.symbol = symbol

    ## Notinggg
    def get_request_analysis_post(self, isContent=True):  # Generate post for DA request
        timeseries_data = dict()
        token_data = self.token_info["symbol"]
        fields = self._get_timeseries_data(list(self.token_info.keys()))
        for field in fields:
            if ("ChangeLogs" in field) & (
                ("activeUsers" not in field) | ("activeHolders" not in field)
            ):
                timeseries_data[field] = self.token_info[field]

        if isContent is True:
            if timeseries_data != {}:
                converted_data = {
                    outer_key: {
                        change_timestamp(int(inner_key)): value
                        for inner_key, value in inner_dict.items()
                    }
                    for outer_key, inner_dict in timeseries_data.items()
                }

                news_prompt_template = RecentAnalysisPromptTemplate()
                template = news_prompt_template.create_template()
                prompt = template.format(
                    analysis_token=token_data,
                    timeseries_data=converted_data,
                )
                response = self.llm.send_prompt(prompt)

            else:
                response = {}

        else:
            return timeseries_data

        return response

    # def get_recent_analysis_post(
    #     self, isContent=True
    # ):  # Generate post based only on the relavent short term timeseries data
    #     timeseries_data = dict()
    #     token_data = self.token_info["symbol"]
    #     fields = self._get_timeseries_data(list(self.token_info.keys()))
    #     for field in fields:
    #         if ("ChangeLogs" in field) & (
    #             ("activeUsers" not in field) | ("activeHolders" not in field)
    #         ):
    #             specific_fields = field[: -len("ChangeLogs")]
    #             if (
    #                 str(round_timestamp(time.time()))
    #                 in self.token_info[f"{specific_fields}AnomalPoints"]
    #             ) | (
    #                 str(round_timestamp(time.time() - 86400))
    #                 in self.token_info[f"{specific_fields}AnomalPoints"]
    #             ):
    #                 timeseries_data[field] = self.token_info[field]

    #     if isContent is True:
    #         if timeseries_data != {}:
    #             converted_data = {
    #                 outer_key: {
    #                     change_timestamp(int(inner_key)): value
    #                     for inner_key, value in inner_dict.items()
    #                 }
    #                 for outer_key, inner_dict in timeseries_data.items()
    #             }

    #             news_prompt_template = RecentAnalysisPromptTemplate()
    #             template = news_prompt_template.create_template()
    #             prompt = template.format(
    #                 analysis_token=token_data,
    #                 timeseries_data=converted_data,
    #             )

    #             response = self.llm.send_prompt(prompt)

    #         else:
    #             response = {}

    #     else:
    #         return timeseries_data

    #     return response, timeseries_data

    def get_period_analysis_post(
        self, isContent=True
    ):  # Generate post based only on the relavent long term timeseries data
        timeseries_data = dict()
        anomal_data = dict()
        fields = self._get_timeseries_data(list(self.token_info.keys()))

        ## preparing timeseries data
        for field in fields:
            if "ChangeLogs" in field:
                originalField = field.split("ChangeLogs")[0]
                if field in ["activeUsersChangeLogs", "activeHoldersChangeLogs"]:
                    if (
                        int(
                            sum(self.token_info[field].values())
                            / len(self.token_info[field])
                        )
                        >= 1000
                    ):
                        timeseries_data[field] = self.token_info[field]
                        anomal_data[f"{originalField}AnomalPoints"] = self.token_info[
                            f"{originalField}AnomalPoints"
                        ]
                    else:
                        continue
                timeseries_data[field] = self.token_info[field]
                anomal_data[f"{originalField}AnomalPoints"] = self.token_info[
                    f"{originalField}AnomalPoints"
                ]

        token_data = self.token_info["symbol"]

        period_df = pd.DataFrame.from_dict(timeseries_data).reset_index(
            names="timestamp"
        )
        period_df["timestamp"] = period_df["timestamp"].apply(
            lambda x: change_timestamp(int(x))
        )

        timeseries_data = period_df.to_dict(orient="records")

        ## preparing anomal data
        converted_anomal_data = {
            key: [change_timestamp(int(ts)) for ts in values]
            for key, values in anomal_data.items()
        }

        if isContent is True:  ## return context, replies, timeseries data
            news_prompt_template = PeriodAnalysisPromptTemplate()
            template = news_prompt_template.create_template()
            prompt = template.format(
                analysis_token=token_data,
                timeseries_data=timeseries_data,
                anomal_points=converted_anomal_data,
            )

            response, replies = self._gen_response(prompt=prompt)

            return response, replies, timeseries_data

        else:  ## only return timeseries data and anomal data
            return timeseries_data, converted_anomal_data

    def get_summarized_top_token_post(
        self, tweets
    ):  # Generate post based on the relavent tweets posts and timeseries data
        timeseries_data = dict()
        anomal_data = dict()

        ## preparing timeseries data
        fields = self._get_timeseries_data(list(self.token_info.keys()))

        for field in fields:
            if "ChangeLogs" in field:
                originalField = field.split("ChangeLogs")[0]
                if field in ["activeUsersChangeLogs", "activeHoldersChangeLogs"]:
                    if (
                        int(
                            sum(self.token_info[field].values())
                            / len(self.token_info[field])
                        )
                        >= 1000
                    ):
                        timeseries_data[field] = self.token_info[field]
                        anomal_data[f"{originalField}AnomalPoints"] = self.token_info[
                            f"{originalField}AnomalPoints"
                        ]
                    else:
                        continue
                timeseries_data[field] = self.token_info[field]
                anomal_data[f"{originalField}AnomalPoints"] = self.token_info[
                    f"{originalField}AnomalPoints"
                ]

        period_df = pd.DataFrame.from_dict(timeseries_data).reset_index(
            names="timestamp"
        )
        period_df["timestamp"] = period_df["timestamp"].apply(
            lambda x: change_timestamp(int(x))
        )

        timeseries_data = period_df.to_dict(orient="records")

        news_prompt_template = TrendingMentionedTokenPromptTemplate()
        template = news_prompt_template.create_template()
        prompt = template.format(
            symbol=self.symbol,
            tweets=tweets,
            timeseries_data=timeseries_data,
        )

        response, replies = self._gen_response(prompt=prompt)

        # Return responses, replies, and used materials: tweets and timeseries data
        return response, replies, timeseries_data

    def get_keyword_summarized_tweet_post(
        self, tweets
    ):  # Generate post based on the input keyword
        prompt_template = KeywordMentionedPromptTemplate()
        template = prompt_template.create_template()
        prompt = template.format(keyword=self.symbol, tweets=tweets)

        response, replies = self._gen_response(prompt=prompt)
        return response, replies

    def _get_timeseries_data(self, fields):
        change_logs_suffix = "ChangeLogs"
        anomal_points_suffix = "AnomalPoints"
        change_logs = {
            f[: -len(change_logs_suffix)]
            for f in fields
            if f.endswith(change_logs_suffix)
        }
        anomal_points = {
            f[: -len(anomal_points_suffix)]
            for f in fields
            if f.endswith(anomal_points_suffix)
        }
        common_bases = change_logs & anomal_points

        # Construct the filtered list
        filtered_fields = {base + change_logs_suffix for base in common_bases} | {
            base + anomal_points_suffix for base in common_bases
        }

        return filtered_fields

    def _gen_response(self, prompt):
        """
        generate response, including check the format of the post
        """

        replies = []
        check = False
        while not check:
            responses = self.llm.send_prompt(prompt).split("//")
            check = self._check_format(responses=responses)
            if check:
                for idx in range(1, len(responses)):
                    replies.append(responses[idx])

        return responses[0], replies

    def _check_breakline(self, txt):
        """ "
        Check if right format, rule of this format:
        - Each break line should not exceed the length of two lines on Twitter.
        """
        if len(txt) > 2 * TwitterTweetConstant.LENGTH_PER_LINE:
            txt_split = txt.split("\n")
            if len(txt_split) <= 1:
                return False
            else:
                for txt in txt_split:
                    if len(txt) > 2 * TwitterTweetConstant.LENGTH_PER_LINE:
                        return False

        return True

    def _check_format(self, responses):
        """
        return True if content statisfied breakline and smaller than maximum tweet length

        """
        for idx in range(0, len(responses)):
            if (not self._check_breakline(responses[idx])) | (
                len(responses[idx]) > TwitterTweetConstant.BASIC_MAXIMUM_LENGTH
            ):
                return False

        return True
