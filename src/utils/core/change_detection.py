import time
from collections import defaultdict

import numpy as np
import pandas as pd
import ruptures as rpt
from adtk.detector import GeneralizedESDTestAD
from IPython.utils import io
from matplotlib import pyplot as plt
from scipy.stats import linregress
from statsmodels.tsa.seasonal import seasonal_decompose

from src.constants.time import TimeConstants
from src.utils.time import round_timestamp


# system
def append_value(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)


## correlation
def map_corr_message(group, corr_groups, sign):  ## get correlation message
    txt_lst = []
    if len(corr_groups) > 0:
        if group in ["Competitor", "Potential"]:
            for corr_group in corr_groups:
                action_txt = ""
                for action in corr_group:
                    action_txt += f" {action},"
                action_txt = action_txt[:-1]

                txt_lst.append(
                    f"In top {group} used by Users, there are a highly {sign} correlation between: {action_txt}"
                )
        elif group == "UsagePerformance":
            for corr_group in corr_groups:
                action_txt = ""
                for action in corr_group:
                    action_txt += f" {action},"
                action_txt = action_txt[:-1]

                txt_lst.append(
                    f"In the Usage Performance, there are a highly {sign} correlation between: {action_txt}"
                )
        elif group in ["Action-Dapps_Users", "Action-Dapps_Transactions"]:
            name = group.split("-")[-1]
            for corr_group in corr_groups:
                action_txt = ""
                for action in corr_group:
                    action_txt += f" {action},"
                action_txt = action_txt[:-1]

                txt_lst.append(
                    f"In the trending activities of {name}, there are a highly {sign} correlation between: {action_txt}"
                )
        else:
            if group in ["Action_Users", "Action_Transactions"]:
                name = group.replace("Action", "")
                for corr_group in corr_groups:
                    action_txt = ""
                for action in corr_group:
                    action_txt += f" {action},"
                action_txt = action_txt[:-1]

                txt_lst.append(
                    f"In the distribution activities of {name}, there are a highly {sign} correlation between: {action_txt}"
                )

            elif group in ["Contract_Users", "Contract_Transactions"]:
                name = group.replace("Contract", "")
                for corr_group in corr_groups:
                    action_txt = ""
                for action in corr_group:
                    action_txt += f" {action},"
                action_txt = action_txt[:-1]

                txt_lst.append(
                    f"In the distribution activities of {name}, there are a highly {sign} correlation between: {action_txt}"
                )

            elif group in ["Dapps_Users", "Dapps_Transactions"]:
                name = group.replace("Dapps", "")
                for corr_group in corr_groups:
                    action_txt = ""
                for action in corr_group:
                    action_txt += f" {action},"
                action_txt = action_txt[:-1]

                txt_lst.append(
                    f"In the distribution activities of {name}, there are a highly {sign} correlation between: {action_txt}"
                )

    return txt_lst


def sort_correlation(corre_dict, rank_features):
    sorted_values = [corre_dict[key] for key in rank_features if key in corre_dict]
    return sorted_values


def check_correlation_groups(correlation_lst):
    # Separate tuples into positive and negative score lists
    pos_tuples = [t for t in correlation_lst if t[2] > 0]
    neg_tuples = [t for t in correlation_lst if t[2] < 0]

    def build_adjacency_list(tuples):
        adj_list = defaultdict(set)
        for name1, name2, _ in tuples:
            adj_list[name1].add(name2)
            adj_list[name2].add(name1)
        return adj_list

    def find_connected_components(adj_list):
        visited = set()
        components = []

        def dfs(node, component):
            stack = [node]
            while stack:
                n = stack.pop()
                if n not in visited:
                    visited.add(n)
                    component.append(n)
                    stack.extend(adj_list[n] - visited)

        for node in adj_list:
            if node not in visited:
                component = []
                dfs(node, component)
                components.append(component)

        return components

    # Build adjacency lists for positive and negative scores
    pos_adj_list = build_adjacency_list(pos_tuples)
    neg_adj_list = build_adjacency_list(neg_tuples)

    # Find connected components in each adjacency list
    pos_groups = find_connected_components(pos_adj_list)
    neg_groups = find_connected_components(neg_adj_list)

    return pos_groups, neg_groups


def get_corr_pair(correlation_matrix, threshold):
    high_corr_pairs = []
    for i in range(len(correlation_matrix.columns)):
        for j in range(i):
            if abs(correlation_matrix.iloc[i, j]) > threshold:
                feature1 = correlation_matrix.columns[i].split(" & ")
                feature2 = correlation_matrix.columns[j].split(" & ")
                corr_value = correlation_matrix.iloc[i, j]
                if len(feature1) > 1:
                    high_corr_pairs.append((feature1[1], feature2[1], corr_value))
                else:
                    high_corr_pairs.append((feature1[0], feature2[0], corr_value))
    return high_corr_pairs


# ranking


def ranking_tag(tag_trend, tag_volatility):
    if "significant" in tag_trend:
        return 1
    elif "significant" in tag_volatility:
        return 2
    elif ("stable" not in tag_trend) & ("stable" not in tag_volatility):
        return 3
    else:
        return 4


def get_rank_action(dataframe):
    with io.capture_output() as captured:
        action_inf = summarise(dataframe)
        action_group = dict()
        rank_action = dict()
        for action in action_inf.keys():
            group_name = action.split("-")[0]
            action_info = dict()
            action_info[action] = np.median(dataframe[f"{action}"].tolist())
            append_value(action_group, group_name, action_info)
            if len(action_inf[action]["change_points"]) > 0:
                rank_action[action] = len(action_inf[action]["change_points"])

        rank_action = dict(
            sorted(rank_action.items(), key=lambda item: item[1], reverse=True)
        )
    print(rank_action)
    return rank_action


def get_rank_change_point(dataframe):
    with io.capture_output() as captured:
        action_inf = summarise(dataframe)
        action_group = dict()
        rank_action = dict()
        action_changepoint = dict()
        for action in action_inf.keys():
            group_name = action.split("-")[0]
            action_info = dict()
            action_info[action] = np.median(dataframe[f"{action}"].tolist())
            append_value(action_group, group_name, action_info)
            if len(action_inf[action]["change_points"]) > 0:
                rank_action[action] = len(action_inf[action]["change_points"])
                action_changepoint[action] = action_inf[action]["change_points"]

        rank_action = dict(
            sorted(rank_action.items(), key=lambda item: item[1], reverse=True)
        )

    return rank_action, action_changepoint


def get_action_name(action):  ## mapping action
    action_decompose = action.split(" & ")
    if action_decompose[0] in ["Action", "Contract", "Dapps"]:
        distribution = action_decompose[0]
        distri_type = action_decompose[1]
        distri_entity = action_decompose[2]

        return f"the distribution of {distribution} by total number of {distri_entity}, considering the {distri_type}"
    if action_decompose[0] in ["Action - Dapps"]:
        distri_entity = action_decompose[2]
        distri_type = action_decompose[1]
        if distri_entity == "Users":
            return f"the trending activities among users, considering the action {distri_type}"
        else:
            return f"the trending activities among all transactions, considering the action {distri_type}"

    if action_decompose[0] in ["Potential", "Competitor"]:
        distri_type = action_decompose[1]
        if action_decompose[0] == "Potential":
            return f"the top Potential Partners used by your users, considering the action {distri_type}"
        else:
            return f"the top Competitors used by your users, considering the action {distri_type}"

    if action_decompose[0] in ["users", "transactions", "claimRewards", "tvl"]:
        context = action_decompose[0]
        return f"the Usage Performance, considering feature {context}"


def find_anomal_range(
    anomal_points, start_point, end_point
):  # find the anomaly points in a specific range
    anomal_lst = []
    for anomal_point in anomal_points:
        if start_point <= anomal_point <= end_point:
            anomal_lst.append(anomal_point)
    return anomal_lst


def detect_anomaly(dataframe):
    dataframe.columns = ["timestamp", "value"]
    tst_transform = load_data(dataframe, "timestamp", "value")
    esd_ad = GeneralizedESDTestAD(alpha=0.89)
    anomalies = esd_ad.fit_detect(tst_transform).reset_index()
    anomal_idx = anomalies.index[anomalies["value"] == True].tolist()
    return anomal_idx


def get_anomaly_point_lst(dataframe, col):
    sub_df = dataframe[["timestamp", col]]
    anomal_idx = detect_anomaly(sub_df)
    subb_df = dataframe[["timestamp", col]]
    subb_df["timestamp"] = subb_df["timestamp"].apply(lambda x: round_timestamp(int(x)))
    # subb_df[col] = subb_df[col].apply(lambda x: int(x))
    if len(anomal_idx) > 0:
        anomal_times = subb_df.loc[anomal_idx, "timestamp"]
        anomal_lst = list(str(value) for value in anomal_times.values)
    else:
        anomal_lst = []

    return anomal_lst, subb_df


def detect_changes(time_series):  # change point
    signal = time_series.values
    model = "l2"  # "l2", "l1", "rbf", "linear", "normal", "ar",...
    algo = rpt.Binseg(model=model).fit(signal)
    result = algo.predict(pen=len(signal) / 4)
    change_points = [i for i in result if i < len(signal)]
    return change_points


def get_tag_volatility(
    sub_series: np.ndarray, max_change_rate: float, anomaly_lst=list()
):  # variation
    if 0 < len(anomaly_lst) < 5:
        sub_series = sub_series.drop(anomaly_lst)

    std_dev = np.std(sub_series)
    vol_percent = std_dev / max_change_rate

    if vol_percent < 0.05:
        tag_volatility = "stable varied"
    elif 0.05 <= vol_percent < 0.2:
        tag_volatility = "slight varied"
    else:
        tag_volatility = "significant varied"

    return tag_volatility


def get_slope(trend_array: np.ndarray, max_change_rate: float):  # trend
    n = len(trend_array)
    # Get slope of the trend line
    x = np.arange(n)
    slope, intercept, r_value, p_value, std_err = linregress(x, trend_array)
    slope_percent = slope / max_change_rate

    return slope_percent


def get_direction(slope):
    if slope > 0.005:
        if slope < 0.05:
            tag_trend = "a slight increase trend"
        elif 0.05 < slope < 0.1:
            tag_trend = "an increase trend"
        else:
            tag_trend = "a significant increase trend"

        return tag_trend

    elif slope < -0.005:
        if np.abs(slope) < 0.05:
            tag_trend = "a slight decrease trend"
        elif 0.05 < np.abs(slope) < 0.1:
            tag_trend = "a decrease trend"
        else:
            tag_trend = "a significant decrease trend"

        return tag_trend

    elif -0.005 <= slope <= 0.005:
        tag_trend = "a stable trend"

        return tag_trend

    else:
        tag_trend = None

        return tag_trend


def get_tag_trend(sub_series: pd.Series, max_change_rate: float):
    trend_array = np.array(sub_series.values)

    slope_percent = get_slope(trend_array, max_change_rate)

    # point and index
    start_point = trend_array[0]
    timestamp_list = list(sub_series.index)
    other_max_index = list(trend_array).index(np.max(trend_array[1:]))
    other_min_index = list(trend_array).index(np.min(trend_array[1:]))
    # date
    start_date = timestamp_list[0]
    end_date = timestamp_list[-1]
    max_date = timestamp_list[other_max_index]
    min_date = timestamp_list[other_min_index]

    if -0.005 <= slope_percent <= 0.005:
        tag_trend = f"there was a stable trend from {start_date} to {end_date}"

    else:
        if other_max_index >= other_min_index:  ## min start before max
            if start_point < trend_array[other_min_index]:  ## always increase
                if slope_percent < 0.05:
                    tag_trend = f"there was a slight increase trend from {start_date} to {end_date}"
                elif 0.05 < slope_percent < 0.1:
                    tag_trend = (
                        f"there was an increase trend from {start_date} to {end_date}"
                    )
                else:
                    tag_trend = f"there was a significant increase trend from {start_date} to {end_date}"
            else:
                min_trend_array = trend_array[
                    : other_min_index + 1
                ]  ## array from start point to min point
                min_slope_percent = get_slope(min_trend_array, max_change_rate)
                min_tag = get_direction(min_slope_percent)

                max_trend_array = trend_array[
                    other_min_index:
                ]  ## array from max point to end point
                max_slope_percent = get_slope(max_trend_array, max_change_rate)
                max_tag = get_direction(max_slope_percent)

                tag_trend = f"there was {min_tag} from {start_date} to {min_date} before {max_tag} from {min_date} to {end_date}"

        if other_min_index >= other_max_index:  ## max start before min
            if start_point > trend_array[other_max_index]:  ## always decrease
                if np.abs(slope_percent) < 0.05:
                    tag_trend = f"there was a slight decrease trend from {start_date} to {end_date}"
                elif 0.05 < np.abs(slope_percent) < 0.1:
                    tag_trend = (
                        f"there was an decrease trend from {start_date} to {end_date}"
                    )
                else:
                    tag_trend = f"there was a significant decrease trend from {start_date} to {end_date}"
            else:
                max_trend_array = trend_array[
                    : other_max_index + 1
                ]  ## array from start point to max point
                max_slope_percent = get_slope(max_trend_array, max_change_rate)
                max_tag = get_direction(max_slope_percent)

                min_trend_array = trend_array[
                    other_max_index:
                ]  ## array from min point to end point
                min_slope_percent = get_slope(min_trend_array, max_change_rate)
                min_tag = get_direction(min_slope_percent)

                tag_trend = f"there was {max_tag} from {start_date} to {max_date} before {min_tag} from {max_date} to {end_date}"

    return tag_trend


def get_tag_trend_v2(sub_series: pd.Series, max_change_rate: float):
    trend_array = np.array(sub_series.values)
    slope_percent = get_slope(trend_array, max_change_rate)
    timestamp_list = list(sub_series.index)
    start_date = timestamp_list[0]
    end_date = timestamp_list[-1]

    if -0.005 <= slope_percent <= 0.005:
        tag_trend = f"a stable trend from {start_date} to {end_date}"

    direction = get_direction(slope_percent)
    tag_trend = f"{direction} from {start_date} to {end_date}"

    return tag_trend


def get_tag(series: pd.Series, anomal_lst=[]):
    tag_lst = [0, 1, 2, 3, 4, 5]
    comments = list()
    change_points = detect_changes(series)
    data = np.array(series)
    median_point = np.median(data)
    # Calculate median change rate
    change_rate_lst = list(map(lambda x: np.abs(x - median_point), data))
    max_change_rate = np.max(change_rate_lst)
    # median_change_rate = np.median(change_rate_lst)

    decomposition = seasonal_decompose(
        series, model="additive", period=7, extrapolate_trend="freq"
    )
    decomposition_trend = decomposition.trend
    decomposition_median_point = np.median(decomposition_trend)

    decompose_change_rate_lst = list(
        map(lambda x: np.abs(x - decomposition_median_point), decomposition_trend)
    )
    max_decompose_change_rate = np.max(decompose_change_rate_lst)

    change_points.append(0)
    change_points = sorted(change_points)

    for length_point in range(len(change_points)):
        if change_points[length_point] == change_points[-1]:
            start = change_points[length_point]
            end = None
        else:
            start = change_points[length_point]
            end = change_points[length_point + 1] + 1
        cmt = dict()
        sub_series = series[start:end]
        sub_decom = decomposition_trend[start:end]
        tag_trend = get_tag_trend(
            sub_series=sub_decom, max_change_rate=max_decompose_change_rate
        )
        # vola
        tag_volatility = get_tag_volatility(
            sub_series=sub_series, max_change_rate=max_change_rate
        )
        # anomal
        if len(anomal_lst) > 0:
            timestamp_lst = sub_series.reset_index()["timestamp"].tolist()
            start_date = min(timestamp_lst)
            end_date = max(timestamp_lst)
            tag_anomal = find_anomal_range(
                anomal_points=anomal_lst, start_point=start_date, end_point=end_date
            )
            if len(tag_anomal) > 0:
                cmt_anomal = ""
                for tag in tag_anomal:
                    cmt_anomal += f" {tag},"
                cmt_anomal = cmt_anomal[:-1]
                trends = (
                    tag_trend
                    + " with "
                    + tag_volatility
                    + f" with anomal points: {cmt_anomal}"
                )
                cmt["trend"] = trends
                # cmt['tag'] = ranking_tag(tag_trend, tag_volatility)
                ## if contain anomal, +1 rank
                tagging = ranking_tag(tag_trend, tag_volatility)
                tag_index = tag_lst.index(tagging)
                cmt["tag"] = tag_lst[tag_index - 1]

                comments.append(cmt)
            else:
                trends = tag_trend + " with " + tag_volatility
                cmt["trend"] = trends
                cmt["tag"] = ranking_tag(tag_trend, tag_volatility)
                comments.append(cmt)
        else:
            trends = tag_trend + " with " + tag_volatility
            cmt["trend"] = trends
            cmt["tag"] = ranking_tag(tag_trend, tag_volatility)
            comments.append(cmt)

    return comments


def get_comments(dataframe: pd.DataFrame, action):
    comments = []
    tag_lst = [0, 1, 2, 3, 4, 5]
    sub_df = dataframe[["timestamp", action]]
    series = pd.Series(
        sub_df[["timestamp", action]][f"{action}"].values,
        index=sub_df[["timestamp", action]].timestamp,
    )
    anomal_idx = detect_anomaly(sub_df)
    sub_df = dataframe[["timestamp", action]]
    if len(anomal_idx) > 0:
        anomal_times = sub_df.loc[anomal_idx, "timestamp"]
        anomal_lst = list(anomal_times.values)
    else:
        anomal_lst = []

    change_points = detect_changes(series)
    data = np.array(series)
    median_point = np.median(data)
    # Calculate median change rate
    if action == "tvl":
        change_rate_lst = list(map(lambda x: np.abs(x - median_point), data))
        max_change_rate = np.max(change_rate_lst)

    else:
        max_change_rate = median_point

    change_points.append(0)
    change_points = sorted(change_points)
    for length_point in range(len(change_points)):
        if change_points[length_point] == change_points[-1]:
            start = change_points[length_point]
            end = None
        else:
            start = change_points[length_point]
            end = change_points[length_point + 1] + 1
        cmt = dict()
        sub_series = series[start:end]

        # anomal
        timestamp_lst = sub_series.reset_index()["timestamp"].tolist()
        start_date = min(timestamp_lst)
        end_date = max(timestamp_lst)
        sub_anomal_lst = find_anomal_range(
            anomal_points=anomal_lst, start_point=start_date, end_point=end_date
        )

        # trend
        decomposition = seasonal_decompose(
            sub_series,
            model="additive",
            period=int(len(sub_series) / 2),
            extrapolate_trend="freq",
        )
        decomposition_trend = decomposition.trend
        decomposition_median_point = np.median(decomposition_trend)

        decompose_change_rate_lst = list(
            map(lambda x: np.abs(x - decomposition_median_point), decomposition_trend)
        )
        max_decompose_change_rate = np.max(decompose_change_rate_lst)

        print(max_decompose_change_rate)
        tag_trend = get_tag_trend_v2(
            sub_series=decomposition_trend, max_change_rate=max_decompose_change_rate
        )
        # vola
        tag_volatility = get_tag_volatility(
            sub_series=sub_series,
            max_change_rate=max_change_rate,
            anomaly_lst=sub_anomal_lst,
        )

        rank_tag = ranking_tag(
            tag_trend=tag_trend, tag_volatility=tag_volatility
        )  # getting rank of comment

        cmt["trend"] = tag_trend
        cmt["vola"] = tag_volatility

        if (
            len(sub_anomal_lst) > 0
        ):  # upgrade rank of comment if it contain anomaly points
            tag_index = tag_lst.index(rank_tag)
            upgrade_rank_tag = tag_lst[tag_index - 1]
            positive_points, negative_points = get_anomaly_direction(
                series=sub_series, anomal_lst=sub_anomal_lst
            )

        else:
            upgrade_rank_tag = rank_tag
            positive_points = []
            negative_points = []

        cmt["rank"] = upgrade_rank_tag

        if length_point == len(change_points) - 1:  ## if the last period
            cmt["rank"] = upgrade_rank_tag - 1

        cmt["positive_anomal"] = positive_points
        cmt["negative_anomal"] = negative_points
        comments.append(cmt)

    return comments


def load_data(df, x_column, y_column):
    df.index = pd.to_datetime(df[x_column], unit="s")
    df.drop(columns=x_column, inplace=True)
    df[y_column] = df[y_column].astype(float)
    return df


def get_trend(dataframe, action):
    action_info = list()
    with io.capture_output() as captured:
        #  for action in rank_action:
        sub_df = dataframe[["timestamp", action]]
        seriess = pd.Series(
            sub_df[["timestamp", action]][f"{action}"].values,
            index=sub_df[["timestamp", action]].timestamp,
        )
        anomal_idx = detect_anomaly(sub_df)
        sub_df = dataframe[["timestamp", action]]
        if len(anomal_idx) > 0:
            anomal_times = sub_df.loc[anomal_idx, "timestamp"]
            anomal_lst = list(anomal_times.values)
        else:
            anomal_lst = []
        trends = get_tag(series=seriess, anomal_lst=anomal_lst)
        for trend in trends:
            trend_dct = dict()
            context = trend["trend"]
            trend_type = trend["tag"]
            action_name = get_action_name(action=action)
            cmt = f"In {action_name}, {context}"
            trend_dct["trend"] = cmt
            trend_dct["tag"] = trend_type
            action_info.append(trend_dct)

    return action_info


def summarise(dataframe):
    action_info = dict()
    with io.capture_output() as captured:
        for col in dataframe.columns:
            if col not in ["_id", "timestamp", "chainId"]:
                sub_df = dataframe[["timestamp", col]]
                seriess = pd.Series(
                    sub_df[["timestamp", col]][f"{col}"].values,
                    index=sub_df[["timestamp", col]].timestamp,
                )
                change_idx = detect_changes(seriess)
                anomal_idx = detect_anomaly(sub_df)
                sub_df = dataframe[["timestamp", col]]
                trends = get_tag(series=seriess)
                info = dict()
                if len(change_idx) > 0:
                    change_times = sub_df.loc[change_idx, "timestamp"]
                    info["change_points"] = list(change_times.values)

                if len(anomal_idx) > 0:
                    anomal_times = sub_df.loc[anomal_idx, "timestamp"]
                    info["anomaly_points"] = list(anomal_times.values)

                if "change_points" not in info:
                    info["change_points"] = []

                if "anomaly_points" not in info:
                    info["anomaly_points"] = []

                info["trends"] = trends
                action_info[col] = info

    return action_info


def get_anomaly_direction(
    series, anomal_lst
):  ## return the direction of anomaly point in a series
    series_filter = series.drop(anomal_lst)
    median_point = np.median(series_filter.values)
    positive_list = []
    negative_list = []
    for idx in anomal_lst:
        if series[idx] < median_point:
            negative_list.append(idx)
        else:
            positive_list.append(idx)

    return positive_list, negative_list


## AI Agent utils
def calculate_volatility(prices):
    """
    Calculate the volatility of a series of prices.

    Parameters:
    prices (list or numpy array): A series of price values.

    Returns:
    float: Annualized volatility (assuming daily prices).
    """
    if len(prices) < 2:
        return "Not enough data to calculate volatility"

    returns = np.diff(prices) / prices[:-1]
    daily_volatility = np.std(returns, ddof=1)

    return daily_volatility


def calculate_change_rate(row, prev_row):
    if prev_row is None:
        return None
    return (row["timeseriesColumn"] - prev_row["timeseriesColumn"]) / prev_row[
        "timeseriesColumn"
    ]


def get_anomaly_columns(row):
    """
    Get daily volatility, uptodate_anomal_volatility, anomal points columns
    """
    if row == 0:
        return 0, 0, [], {}

    time_now_lst = [str(round_timestamp(time.time()))]
    seven_days_ago = round_timestamp(time.time()) - round_timestamp(
        TimeConstants.DAYS_7
    )
    df = pd.DataFrame(list(row.items()), columns=["timestamp", "timeseriesColumn"])
    df["timestamp"] = df["timestamp"].apply(lambda x: round_timestamp(x))

    df = df[df["timestamp"] >= seven_days_ago].reset_index(drop=True)
    df = df.groupby("timestamp").mean().reset_index()

    if (df.empty) | ((df["timeseriesColumn"] == 0).any()):
        return 0, 0, [], {}

    else:
        anomal_lst, dff = get_anomaly_point_lst(df, "timeseriesColumn")
        if len(anomal_lst) == 0:
            return (
                0,
                0,
                [],
                dict(zip(df["timestamp"].apply(str), df["timeseriesColumn"])),
            )

        else:
            dff = dff.sort_values("timestamp")
            timeseries_lst = dff["timeseriesColumn"].tolist()
            seven_daily_volatility_rate = calculate_volatility(timeseries_lst)
            uptodate_anomaly_point = set(time_now_lst).intersection(set(anomal_lst))
            if len(uptodate_anomaly_point) > 0:
                sub_df = dff.iloc[-2:].reset_index(drop=True)
                sub_df["changeRate"] = sub_df.apply(
                    lambda row: calculate_change_rate(
                        row, sub_df.iloc[row.name - 1] if row.name > 0 else None
                    ),
                    axis=1,
                )

                sub_df = sub_df.dropna()
                update_anomal_volatility_rate = sub_df[
                    ["timestamp", "changeRate"]
                ].iloc[-1, -1]

                return (
                    seven_daily_volatility_rate,
                    update_anomal_volatility_rate,
                    anomal_lst,
                    dict(zip(df["timestamp"].apply(str), df["timeseriesColumn"])),
                )
            else:
                return (
                    seven_daily_volatility_rate,
                    0,
                    anomal_lst,
                    dict(zip(df["timestamp"].apply(str), df["timeseriesColumn"])),
                )


## ploting
def plot_anomaly(anomaly_index, series):
    plt.figure(figsize=(10, 6))

    # Plot the timeseries data
    plt.plot(series.index, series, label="signal")

    # Highlight the specific points
    for date in anomaly_index:
        plt.scatter(date, series.loc[date], color="red")
        plt.annotate(
            f"({date}, {series.loc[date]:.2f})",
            (date, series.loc[date]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            color="red",
        )

    # Add labels and title
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.title("Timeseries Data with Highlighted Points")
    plt.legend()

    plt.show()
