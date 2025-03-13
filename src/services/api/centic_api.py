import numpy as np
import pandas as pd
import requests

from src.constants.config import CenticApiKey
from src.utils.time import change_timestamp, round_timestamp


class CenticApi:
    def __init__(
        self,
        prj_name,
        start=0,
        end=0,
        cdp_data="",
        campaign_data="",
        campaign_id="",
        chain_id="",
        contract="",
        dapps="",
        token_id="",
    ):
        self.cdp_data = cdp_data
        self.campaign_data = campaign_data
        self.prj_name = prj_name
        self.start = start
        self.end = end

        self.campaign_id = campaign_id
        self.chain_id = chain_id
        self.token_id = token_id

        self.contract = contract
        self.dapps = dapps

        self.api_key = CenticApiKey.APIKEY
        self.authorization = CenticApiKey.JWT

    def get_api_data(self, url):
        headers = {
            "x-apikey": self.api_key,
        }
        response = requests.get(url, headers=headers)
        return response.json()

    def get_campaign_data(self, url):
        headers = {"authorization": self.authorization}
        response = requests.get(url, headers=headers)
        return response.json()

    def get_tvl(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}?&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)["tvlChangeLogs"]
        tvl_df = pd.DataFrame()
        tvl_df["timestamp"] = list(data.keys())
        tvl_df["tvl"] = list(data.values())

        return tvl_df

    def get_reward(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/daily-claimed-rewards?chainId={self.chain_id}&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)["claimed_rewards"]
        reward_df = pd.DataFrame()
        reward_df["timestamp"] = list(data.keys())
        reward_df["reward"] = list(data.values())

        return reward_df

    def get_unique_active_user(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/daily-users?chainId={self.chain_id}&contract={self.contract}&dapp=&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)["dailyUsers"]
        unique_active_user = pd.DataFrame(data)
        return unique_active_user[["timestamp", "numberOfUsers"]]

    def get_distribution_by_action(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/distribution-by-actions?chainId={self.chain_id}&contract={self.contract}&dapp={self.dapps}"
        data = self.get_api_data(url)["actions"]

        return data

    def get_distribution_by_dapps(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/distribution-by-dapps?chainId={self.chain_id}&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)["dapps"]

        return data

    def get_similar_dapps_data(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/use-competitors?chainId={self.chain_id}&contract={self.contract}&dapp={self.dapps}"
        data = self.get_api_data(url)["marketShare"]

        return data

    def get_distribution_by_contract(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/distribution-by-contracts?chainId={self.chain_id}&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)["contracts"]

        return data

    def get_distribution_by_tokens(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/distribution-by-tokens?chainId={self.chain_id}&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)

        return data

    def get_new_return(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/cdp-users/user-inbounce?endTime={self.end}&startTime={self.start}&contract={self.contract}&dapp={self.dapps}"
        data = self.get_api_data(url)

        return data

    def get_competitors_users(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/use-competitors?chainId={self.chain_id}&contract={self.contract}&dapp={self.dapps}"
        data = self.get_api_data(url)["users"]

        return data

    def get_potential_users(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/use-partners?chainId={self.chain_id}&contract={self.contract}&dapp={self.dapps}"
        data = self.get_api_data(url)["users"]

        return data

    def get_trending_activities(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/daily-actions?chainId=&contract=&dapp=&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)["dailyActions"]

        df_lst = []
        for i in range(len(data)):
            df = pd.DataFrame(data[i]["actions"])
            df["timestamp"] = data[i]["timestamp"]
            df_lst.append(df)

        act_df = (
            pd.concat(df_lst)
            .groupby(["timestamp", "name"])
            .agg({"numberOfUsers": "sum"})
            .reset_index()
        )

        return act_df

    def get_retention(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/cdp-users/retention?endTime={self.end}&contract={self.contract}&dapp={self.dapps}&timeRange=7&unitTimeRange=day&timeGap=day"
        data = self.get_api_data(url)

        return data

    def get_holder_classification_by_balance_and_time(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/token-balance-distribution?chainId=&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_holder_distribution_by_prominent_foundation(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/prominent-balance?type=foundation&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_holder_distribution_by_prominent_contract(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/prominent-balance?type=contract&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_holder_distribution_by_prominent_wallet(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/prominent-balance?type=wallet&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_wallet_holders(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/holder-change?chainId={self.chain_id}&startTime={self.start}&endTime={self.end}&tokenId={self.token_id}"
        data = self.get_api_data(url)["logs"]

        timestamp_lst = list(data.keys())
        active_users_lst = [i["active"] for i in list(data.values())]
        new_users_lst = [i["new"] for i in list(data.values())]

        wallet_holder_df = pd.DataFrame()
        wallet_holder_df["timestamp"] = timestamp_lst
        wallet_holder_df["newUsers"] = new_users_lst
        wallet_holder_df["activeUsers"] = active_users_lst

        return wallet_holder_df

    def get_trending_addresses(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/event-transfer-distribution"
        data = self.get_api_data(url)

        df_aggregate = pd.DataFrame()
        timestamp_lst = []
        name_lst = []
        value_lst = []
        for timestamp, addresses_dct in data.items():
            for _, address_info in addresses_dct.items():
                timestamp_lst.append(timestamp)
                name_lst.append(address_info["name"])
                value_lst.append(address_info["value"])

        df_aggregate["timestamp"] = timestamp_lst
        df_aggregate["name"] = name_lst
        df_aggregate["value"] = value_lst

        df_aggregate = (
            df_aggregate.groupby(["timestamp", "name"])
            .agg({"value": "sum"})
            .reset_index()
        )

        return df_aggregate

    def get_popular_tokens(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/defi-distribution?defiType=token&holderType=all&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_popular_NFTs(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/defi-distribution?defiType=nft&holderType=all&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_popular_dapps(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/holders/defi-distribution?defiType=dapp&holderType=all&chainId={self.chain_id}&tokenId={self.token_id}"
        data = self.get_api_data(url)

        return data

    def get_trending_tokens(self):
        url = f"https://develop.centic.io/stag/v3/data/{self.prj_name}/dapp-users/daily-tokens?chainId={self.chain_id}&startTime={self.start}&endTime={self.end}"
        data = self.get_api_data(url)

        timestamp_lst_inflow = []
        name_lst_inflow = []
        value_lst_inflow = []
        df_aggregate_inflow = pd.DataFrame()
        for addresses_dct in data["inflowActions"]:
            for token_info in addresses_dct["actions"]:
                timestamp_lst_inflow.append(addresses_dct["timestamp"])
                name_lst_inflow.append(token_info["name"])
                value_lst_inflow.append(token_info["totalValue"])

        df_aggregate_inflow["timestamp"] = timestamp_lst_inflow
        df_aggregate_inflow["name"] = name_lst_inflow
        df_aggregate_inflow["totalValue"] = value_lst_inflow

        df_aggregate_inflow = (
            df_aggregate_inflow.groupby(["timestamp", "name"])
            .agg({"totalValue": "sum"})
            .reset_index()
        )

        timestamp_lst_outflow = []
        name_lst_outflow = []
        value_lst_outflow = []
        df_aggregate_outflow = pd.DataFrame()
        for addresses_dct in data["outflowActions"]:
            for token_info in addresses_dct["actions"]:
                timestamp_lst_outflow.append(addresses_dct["timestamp"])
                name_lst_outflow.append(token_info["name"])
                value_lst_outflow.append(token_info["totalValue"])

        df_aggregate_outflow["timestamp"] = timestamp_lst_outflow
        df_aggregate_outflow["name"] = name_lst_outflow
        df_aggregate_outflow["totalValue"] = value_lst_outflow

        df_aggregate_outflow = (
            df_aggregate_outflow.groupby(["timestamp", "name"])
            .agg({"totalValue": "sum"})
            .reset_index()
        )

        return df_aggregate_inflow, df_aggregate_outflow

    ## CDP Social Media
    # Twitter
    def get_audience_over_time_data(self):
        data = self.cdp_data[0]
        df = pd.DataFrame()
        df["timestamp"] = data["twitterFollowersChangeLogs"].keys()
        df["twitterFollowes"] = data["twitterFollowersChangeLogs"].values()
        df["telegramFollowes"] = data["telegramMembersChangeLogs"].values()

        return df

    def get_engagement_over_time_data(self):
        data = self.cdp_data[1]
        data_over_time = data.get("projectEngagementOverTime", {})
        df = pd.DataFrame()
        df["timestamp"] = data_over_time["favouritesCount"].keys()
        df["favouritesCount"] = data_over_time["favouritesCount"].values()
        df["friendsCount"] = data_over_time["friendsCount"].values()
        df["listedCount"] = data_over_time["listedCount"].values()
        df["mediaCount"] = data_over_time["mediaCount"].values()
        df["statusesCount"] = data_over_time["statusesCount"].values()
        return df

    def get_followers_quality_data(self):
        data = self.cdp_data[2]
        data_over_time = data.get("followersQualityChangeLogs", {})

        followers_df = pd.DataFrame()
        followers_df["timestamp"] = data_over_time["Verified"].keys()
        followers_df["verified"] = data_over_time["Verified"].values()
        followers_df["normal"] = data_over_time["Normal"].values()

        return followers_df

    def get_followers_quality_distribution_data(self):
        data = self.cdp_data[2]
        data_distribution = data.get("followersQualityRatio", {})

        return data_distribution

    def get_most_popular_hashtag_data(self):
        data = self.cdp_data[3]
        data_most_popular_hashtag = data.get("mostHashTags", {})
        return data_most_popular_hashtag

    def get_followers_by_location_data(self):
        data = self.cdp_data[4]
        data_follower_by_location = data.get("location", {})

        return data_follower_by_location

    def get_most_liked_posts_data(self):
        data = self.cdp_data[5]
        data_most_liked_posts = data.get("mostLikedPosts", {})

        return data_most_liked_posts

    # Telegram

    ## Campaigns
    def get_usage_metric(self):
        data = self.campaign_data[0]

        df_lst = []
        for timestamp, features in data["history"].items():
            df = pd.DataFrame()
            df["timestamp"] = [timestamp]
            df["visit"] = [features["visit"]]
            df["connectWallet"] = [features["connectWallet"]]
            df["transaction"] = [features["transaction"]]

            df_lst.append(df)

        dff = pd.concat(df_lst).reset_index(drop=True)

        return dff.to_dict(orient="records")

    # c5cd5f0e-d982-4248-9591-80e5fdaad085
    def get_goal_metric(self):
        data = self.campaign_data[1]

        df_lst = []
        for feature in data["objectives"]:
            df = pd.DataFrame()
            df["goal"] = [feature["goal"]]
            df["completion rate"] = [feature["completionRate"]]

            df_lst.append(df)

        dff = pd.concat(df_lst).reset_index(drop=True)

        return dff.to_dict(orient="records")

    def get_session_history(self):
        data = self.campaign_data["sessionHistory"]

        df = pd.DataFrame()
        df["timestamp"] = list(data.keys())
        df["sessionNum"] = list(data.values())

        return df.to_dict(orient="records")

    def get_top_channel(self):
        data = self.campaign_data["channels"]

        df_top_channel = pd.DataFrame()
        channel_lst = list(data.keys())
        ratio_lst = [value_ratio["ratio"] for value_ratio in list(data.values())]

        df_top_channel["channels"] = channel_lst
        df_top_channel["ratio"] = ratio_lst

        return df_top_channel

    def get_funnel(self):
        data = self.campaign_data

        if "comment" in data:
            del data["comment"]

        return data

    def get_traffic(self):
        data = self.campaign_data
        elements_lst = ["User Source", "Apps Websites", "Connect Wallets", "DApps"]

        layer_dict = {}
        for datum in range(len(data["layers"])):
            layer_dict[elements_lst[datum]] = data["layers"][datum]

        flow_lst = [
            "From User Source to Apps Websites",
            "From Apps Websites to Connect Wallet",
            "From Connect Wallets to DApps",
        ]
        flow_dict = {}
        for datim in range(len(data["flows"])):
            flow_dict[flow_lst[datim]] = data["flows"][datim]

        return layer_dict, flow_dict

    def get_social_media(self):
        social_media = ["twitter", "telegram", "discord"]

        data = self.campaign_data

        social_dct = dict()
        for social in social_media:
            if social == "twitter":
                records = []
                for timestamp, value in data[social][
                    f"{social}GrowthFollowerHistory"
                ].items():
                    records.append([change_timestamp(int(timestamp)), value])

            else:
                records = []
                for timestamp, value in data[social][
                    f"{social}GrowthMemberHistory"
                ].items():
                    records.append([change_timestamp(int(timestamp)), value])

            df = pd.DataFrame(records, columns=["timestamp", "followerGrowth"])
            growth_median = np.median(df.followerGrowth.tolist())
            if growth_median == 0:
                continue

            social_info = df.to_dict(orient="records")
            social_dct[social] = social_info

        return social_dct

    def get_twitter(self):
        data = self.campaign_data

        del data["tweets"]
        if "comment" in data:
            del data["comment"]

        records = []
        for timestamp, followers in data["followerHistory"].items():
            records.append(
                [change_timestamp(int(timestamp)), followers["totalFollowers"]]
            )

        del data["followerHistory"]

        follower_df = pd.DataFrame(records)
        follower_df.columns = ["timestamp", "followers"]
        data["followerHitory"] = follower_df.to_dict(orient="records")

        return data

    def get_discord(self):
        data = self.campaign_data

        if "comment" in data:
            del data["comment"]

        records = []
        for timestamp, followers in data["memberHistory"].items():
            records.append(
                [change_timestamp(int(timestamp)), followers["totalMembers"]]
            )

        del data["memberHistory"]

        follower_df = pd.DataFrame(records)
        follower_df.columns = ["timestamp", "totalMembers"]
        data["memberHistory"] = follower_df.to_dict(orient="records")

        return data

    def get_telegram(self):
        data = self.campaign_data

        if "comment" in data:
            del data["comment"]

        records = []
        for timestamp, followers in data["memberHistory"].items():
            records.append(
                [change_timestamp(int(timestamp)), followers["totalMembers"]]
            )

        del data["memberHistory"]

        follower_df = pd.DataFrame(records)
        follower_df.columns = ["timestamp", "totalMembers"]
        data["memberHistory"] = follower_df.to_dict(orient="records")

        return data

    def get_quest(self):
        data = self.campaign_data

        quest_summarise = dict()
        quest_participants = list()
        quests = list(data.keys())[:-1]
        for quest in quests:
            quest_info = dict()
            del data[quest]["growthParticipants"]

            quest_info = data[quest]
            quest_info["platform"] = quest
            quest_participants.append(quest_info)

        quest_summarise["participant"] = quest_participants
        quest_performance = []
        for platform in data["questsPerformance"]:
            del platform["name"]
            for key in list(platform.keys()):
                if (platform[key] == 0) | (platform[key] is None):
                    del platform[key]

            if len(platform) > 2:
                quest_performance.append(platform)

        quest_summarise["questPerformance"] = quest_performance

        return quest_summarise["questPerformance"]

    def get_questN(self):
        data = self.campaign_data

        quest_info = dict()
        for quest in data["questnPerformance"]:
            if quest["type"] == "community":
                quest_info["participant_history"] = quest["members"]
                quest_info["subquest_performance"] = quest["details"]
                quest_info["conversion_rate"] = quest["conversionRate"]

                break

        return quest_info

    def get_taskon_space_data(self):
        data = self.campaign_data

        taskon_df = pd.DataFrame()
        timestamp_lst = []
        participant_lst = []
        qualifier_lst = []
        submitter_lst = []
        visitor_lst = []

        for quest in data["taskonPerformance"]:
            if quest["type"] == "space":
                for timestamp, info in quest["taskonMembers"].items():
                    timestamp_lst.append(timestamp)
                    participant_lst.append(info["participants"])
                    qualifier_lst.append(info["qualifiers"])
                    submitter_lst.append(info["submitters"])
                    visitor_lst.append(info["visitors"])

        taskon_df["timestamp"] = timestamp_lst
        taskon_df["participants"] = participant_lst
        taskon_df["qualifiers"] = qualifier_lst
        taskon_df["submitters"] = submitter_lst
        taskon_df["visitors"] = visitor_lst

        taskon_df["timestamp"] = taskon_df["timestamp"].apply(
            lambda x: round_timestamp(int(x))
        )
        taskon_df["timestamp"] = taskon_df["timestamp"].apply(change_timestamp)
        taskon_df = (
            taskon_df.groupby("timestamp")
            .agg(
                {
                    "participants": "mean",
                    "qualifiers": "mean",
                    "submitters": "mean",
                    "visitors": "mean",
                }
            )
            .reset_index()
        )

        taskon_info = dict()

        taskon_info["performanceHistory"] = taskon_df.to_dict(orient="records")
        taskon_info["subcampaignPerformance"] = data["details"]
        taskon_info["distributedCategory"] = data["distributedCategory"]

        return taskon_info

    def get_zealy_data(self):
        data = self.campaign_data

        zealy_info = dict()
        for zealy in data["zealyPerformance"]:
            if zealy["type"] == "community":
                zealy_info["participantHistory"] = zealy["zealyParticipants"]
                # zealy_info['memberSources'] = zealy['memberSources']
                zealy_info["conversionRate"] = zealy["conversionRate"]

                allmember = sum(zealy["memberSources"].values())
                zealy_info["memberSources"] = {
                    key: value / allmember
                    for key, value in zealy["memberSources"].items()
                }

                break

        return zealy_info

    def get_galxe_data(self):
        data = self.campaign_data

        processed_dict = dict()
        member_info = []
        for campaign in data["galxePerformance"]:
            if campaign["type"] == "space":
                for timestamp, value in campaign["members"].items():
                    value["timestamp"] = timestamp
                    member_info.append(value)

                conversion_rate = campaign["conversionRate"]

            break

        processed_dict["memberChange"] = member_info
        processed_dict["conversionRate"] = conversion_rate

        campaign_df = pd.DataFrame(data["details"])
        total_sum = campaign_df.participants.sum()
        campaign_df["participants"] = campaign_df["participants"] / total_sum

        processed_dict["campaignDistribution"] = campaign_df.to_dict(orient="records")

        category_dct = dict()
        for category, ratio in data["distributedCategory"].items():
            del ratio["counts"]

            subcategory = dict()
            subcategory[category] = ratio

            category_dct.update(subcategory)

        processed_dict["categoryDistribution"] = category_dct

        return processed_dict

    def get_taskon_community_data(self):
        data = self.campaign_data

        taskon_df = pd.DataFrame()
        timestamp_lst = []
        participant_lst = []
        visitor_lst = []

        for quest in data["taskonPerformance"]:
            if quest["type"] == "community":
                for timestamp, info in quest["taskonMembers"].items():
                    timestamp_lst.append(timestamp)
                    participant_lst.append(info["participants"])
                    visitor_lst.append(info["visitors"])

        taskon_df["timestamp"] = timestamp_lst
        taskon_df["participants"] = participant_lst
        taskon_df["visitors"] = visitor_lst

        taskon_df["timestamp"] = taskon_df["timestamp"].apply(
            lambda x: change_timestamp(int(x))
        )

        taskon_info = dict()
        taskon_info["performanceHistory"] = taskon_df.to_dict(orient="records")
        taskon_info["subcampaignPerformance"] = data["details"]
        taskon_info["distributedCategory"] = data["distributedCategory"]

        return taskon_info
