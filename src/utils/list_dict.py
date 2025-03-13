import bisect
import json
import time
from collections import defaultdict
from itertools import islice

from src.utils.logger import get_logger
from src.utils.time import round_timestamp


def filter_none_keys(input_dict: dict):
    none_keys = list()
    for key, value in input_dict.items():
        if value is None:
            none_keys.append(key)

    for key in none_keys:
        input_dict.pop(key, "")

    return input_dict


def flatten_and_concat(tuples):
    flattened = [item for sublist in tuples for item in sublist]

    return flattened


def largest_key_smaller_than(dictionary, value, type="search"):
    if type == "search":
        keys = sorted([int(key) for key in list(dictionary.keys())])
    elif type == "train":
        keys = sorted([key for key in list(dictionary.keys())])
    index = bisect.bisect_left(keys, value) - 1
    if index >= 0:
        return keys[index]
    else:
        return 0


def extend_abi(abi: list, added_abi: list):
    abi_names = {a["name"]: 1 for a in abi if a.get("name")}
    for abi_ in added_abi:
        if abi_.get("name") and abi_["name"] in abi_names:
            continue

        abi.append(abi_)

    return abi


def to_change_logs(d: dict):
    return {int(t): v for t, v in d.items()}


def sorted_dict(d: dict, reverse=False):
    return dict(sorted(d.items(), key=lambda x: x[0], reverse=reverse))


def sort_log(log):
    log = to_change_logs(log)
    log = sorted_dict(log)

    return log


def sort_log_dict(log_dict):
    for key in log_dict:
        log = log_dict[key]
        log_dict[key] = sort_log(log)

    return log_dict


def cut_change_logs(
    change_logs: dict,
    end_time: int = None,
    start_time: int = None,
    duration: int = None,
    alt_value=None,
):
    if not end_time:
        end_time = int(time.time())

    if not start_time:
        if not duration:
            raise ValueError("start_time or duration must be set")
        else:
            start_time = end_time - duration

    change_logs = to_change_logs(change_logs)
    change_logs = sorted_dict(change_logs)
    for t in change_logs.keys():
        if (t < start_time) or (t > end_time):
            change_logs[t] = alt_value

    return change_logs


def chunks(input_list: list, size: int):
    for i in range(0, len(input_list), size):
        yield input_list[i : i + size]


def chunks_dict(data: dict, size=50):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}


def delete_none(_dict):
    """Delete None values recursively from all the dictionaries"""
    for key, value in list(_dict.items()):
        if isinstance(value, dict):
            delete_none(value)
        elif value is None:
            del _dict[key]
        elif isinstance(value, list):
            for v_i in value:
                if isinstance(v_i, dict):
                    delete_none(v_i)

    return _dict


def flatten_dict(d):
    out = {}
    for key, val in d.items():
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            array = []
            for subdict in val:
                if not isinstance(subdict, dict):
                    array.append(subdict)
                else:
                    deeper = flatten_dict(subdict).items()
                    out.update(
                        {str(key) + "." + str(key2): val2 for key2, val2 in deeper}
                    )
            if array:
                out.update({str(key): array})
        else:
            out[str(key)] = val

    return out


def rename_key(list_of_dicts, old_key, new_key):
    return [
        {new_key if k == old_key else k: v for k, v in d.items()} for d in list_of_dicts
    ]


def get_special_wallet_list(hot_path, burn_mint_path):
    logger = get_logger("Getting special wallets jobs")
    # try:
    with open(hot_path, "r") as file:
        hot_wallets = json.load(file)

    hot_wallets_lst = []
    for exchange, data in hot_wallets.items():
        for chain_id, addresses in data["wallets"].items():
            hot_wallets_lst.extend(addresses)

    with open(burn_mint_path, "r") as file:
        mint_burn = json.load(file)
    mint_burn_lst = []

    for chain_id, addresses in mint_burn.items():
        mint_burn_lst.extend(addresses)

    hot_wallets_lst.extend(mint_burn_lst)
    hot_wallets_lst = list(set(hot_wallets_lst))
    logger.info("Done!!!!")
    # except:  # noqa: E722
    #     logger.info("Error in getting special wallet job")

    return hot_wallets_lst


def update_dict_of_dict(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1:
            dict1[key].update(value)
        else:
            dict1[key] = value

    return dict1


def update_list_of_dict(list_of_dicts):
    result = {}
    for d in list_of_dicts:
        if len(d) == 2:  # if d only contains address, chainId fields
            if d["chainId"] not in result:
                result[d["chainId"]] = []
            result[d["chainId"]].append(d["address"])
        else:  # if d contains more fields
            if d["chainId"] not in result:
                result[d["chainId"]] = []
            result[d["chainId"]].append(d)

    return result


def chunk_list(input_list, chunk_size):  # get sublist from list of list
    return [
        input_list[i : i + chunk_size] for i in range(0, len(input_list), chunk_size)
    ]


def have_common_tags(tag1, tag2):
    set1 = set(tag1)
    set2 = set(tag2)
    common_elements = set1.intersection(set2)
    return len(common_elements) > 0


def get_subdictionary(original_dict, keys):
    keys_set = set(keys)
    return {k: v for k, v in original_dict.items() if k in keys_set}


def merging_list_of_dict(dict_list):
    temp_dict = defaultdict(list)
    for current_dict in dict_list:
        for key, value in current_dict.items():
            temp_dict[key].append(value)

    intersected_dict = {}
    for key, sets_list in temp_dict.items():
        if len(sets_list) > 1:
            intersected_dict[key] = set.intersection(*sets_list)
        else:
            intersected_dict[key] = sets_list[0]

    return intersected_dict


def get_value_with_default(d: dict, key, default=None):
    """
    The get_value_with_default function is a helper function that allows us to
    get the value of a key in a dictionary, but if the key does not exist or the value of the key is None, it will
    return the default value. This is useful for when we want to check whether something
    exists in our data structure without having to explicitly write code that checks
    for its existence. For example:

    Args:
        d:dict: Specify the dictionary to be used
        key: Retrieve the value from a dictionary
        default: Set a default value if the key is not found in the dictionary or the value of the key is None

    Returns:
        The value of the key if it exists in the dictionary or the value of the key is None,
        otherwise it returns default

    Doc Author:
        Trelent
    """
    if not d:
        return default

    v = d.get(key)
    if v is None:
        v = default
    return v


def combined_logs(*logs, handler_func=sum, default_value=0):
    timestamps = set()
    for log in logs:
        timestamps.update(list(log.keys()))
    timestamps = sorted(timestamps)
    combined = {}
    current_values = [default_value] * len(logs)
    for t in timestamps:
        for idx, log in enumerate(logs):
            current_values[idx] = log.get(t, current_values[idx])

        combined[t] = handler_func(current_values)

    return combined


def get_logs_changed(change_logs, duration):
    current_time = int(time.time())
    change_logs = coordinate_logs(
        change_logs,
        start_time=current_time - duration,
        end_time=current_time,
        fill_start_value=True,
    )
    if not change_logs:
        return None

    values = list(change_logs.values())
    return values[-1] - values[0]


def coordinate_logs(
    change_logs,
    start_time=0,
    end_time=None,
    frequency=None,
    fill_start_value=False,
    default_start_value=0,
):
    if end_time is None:
        end_timestamp = int(time.time())
    else:
        end_timestamp = end_time

    logs = {}
    last_timestamp = 0
    pre_value = default_start_value
    for t, v in change_logs.items():
        if t is None:
            continue

        if t < start_time:
            pre_value = v
        elif start_time <= t <= end_timestamp:
            last_timestamp = _filter_timestamp_in_range(
                logs, t, v, last_timestamp, frequency
            )
        elif t > end_timestamp:
            break

    logs = _fill_margin(
        logs, start_time, end_time, fill_start_value, pre_value, end_timestamp
    )
    return logs


def _fill_margin(
    logs: dict, start_time, end_time, fill_start_value, pre_value, end_timestamp
):
    if (start_time not in logs) and fill_start_value and (pre_value is not None):
        logs[start_time] = pre_value

    logs = sort_log(logs)

    last_value = list(logs.values())[-1] if logs else None
    if (end_time is None) and (last_value is not None):
        logs[end_timestamp] = last_value

    return logs


def _filter_timestamp_in_range(logs: dict, t, v, last_timestamp, frequency):
    if frequency:
        if round_timestamp(t, frequency) != round_timestamp(last_timestamp, frequency):
            logs[t] = v
            last_timestamp = t
    else:
        logs[t] = v

    return last_timestamp


def divide_into_sublists(lst, n):
    """
    Divides a list into a specified number of sublists with most sublists having equal elements.

    Args:
    - lst: The original list to split.
    - n: The number of sublists to divide into.

    Returns:
    - A list of sublists.
    """
    length = len(lst)
    base_size = length // n
    extra = length % n

    sublists = []
    start = 0

    for i in range(n):
        size = base_size + (1 if i < extra else 0)
        sublists.append(lst[start : start + size])
        start += size

    return sublists


def remove_dicts_with_null_values(lst):
    return [d for d in lst if all(value is not None for value in d.values())]


def check_dict_have_null_values(dict):
    return all(value is not None for value in dict.values())


def aggregate_dicts(dict_list, key):
    "Aggregate dictionaries with the same key"

    aggregated_data = defaultdict(
        lambda: {"tokenAddresses": set(), "deployedChains": set()}
    )

    for entry in dict_list:
        _id = entry[key]
        aggregated_entry = aggregated_data[_id]

        if "tokenAddresses" in entry and entry["tokenAddresses"]:
            aggregated_entry["tokenAddresses"].update(
                entry["tokenAddresses"]
            )  # Use set to ensure uniqueness

        if "deployedChains" in entry and entry["deployedChains"]:
            aggregated_entry["deployedChains"].update(entry["deployedChains"])

    # Convert sets to lists and remove empty lists
    return {
        _id: {k: list(v) for k, v in data.items() if v}
        for _id, data in aggregated_data.items()
    }
