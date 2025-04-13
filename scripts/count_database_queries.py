import time
from collections import defaultdict

import redis
from redis import Redis

from src.constants.time import TimeConstants


def count_db_queries(start_time=None, end_time=None):
    if end_time is None:
        end_time = time.time()
    if start_time is None:
        start_time = time.time() - TimeConstants.DAYS_30

    r: Redis = redis.from_url('redis://:redispw_NabYLTy5tqdEN7Ac@152.42.251.182:6379/0')

    keys = r.keys('requests_count.*.*')
    project_requests = defaultdict(lambda: defaultdict(int))
    daily_requests = defaultdict(lambda: defaultdict(int))
    for key in keys:
        _, project, timestamp = key.decode('utf-8').split('.')
        project = project.split('_')[0].split('-')[0]
        if project.lower() == 'centic':
            continue

        timestamp = int(timestamp)
        if timestamp < start_time or timestamp > end_time:
            continue

        count = int(r.get(key))

        project_requests[project][timestamp] += count
        daily_requests[timestamp][project] += count

    total_requests = sum([sum(info.values()) for info in daily_requests.values()])
    print(f'Total requests: {total_requests}')

    # For another version
    # with open('../.data/project_requests.json', 'w') as f:
    #     json.dump(sort_project_requests(project_requests), f, indent=2)
    #
    # with open('../.data/daily_requests.json', 'w') as f:
    #     json.dump(sort_daily_requests(daily_requests), f, indent=2)

    return {t: sum(info.values()) for t, info in daily_requests.items()}


def sort_project_requests(json_dict):
    for k in json_dict.keys():
        json_dict[k] = dict(sorted(json_dict[k].items(), key=lambda item: item[0]))

    json_dict = dict(sorted(json_dict.items(), key=lambda item: sum(item[1].values()), reverse=True))
    return json_dict


def sort_daily_requests(json_dict):
    for k in json_dict.keys():
        json_dict[k] = dict(sorted(json_dict[k].items(), key=lambda item: item[1], reverse=True))

    json_dict = dict(sorted(json_dict.items(), key=lambda item: item[0]))
    return json_dict


if __name__ == '__main__':
    count_db_queries()
