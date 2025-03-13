import time

from src.utils.time import round_timestamp


class TimeConstants:
    TIME_NOW = round_timestamp(time.time())
    TIME_AT_THE_MOMENT = time.time()
    A_MINUTE = 60
    MINUTES_5 = 300
    MINUTES_15 = 900
    A_HOUR = 3600
    A_DAY = 86400
    DAYS_1_5 = 1.5 * A_DAY
    DAYS_2 = 2 * A_DAY
    DAYS_3 = 3 * A_DAY
    DAYS_5 = 5 * A_DAY
    DAYS_7 = 7 * A_DAY
    DAYS_30 = 30 * A_DAY
    DAYS_31 = 31 * A_DAY
    MONTHS_3 = 90 * A_DAY
    A_YEAR = 365 * A_DAY
