from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from plot_weather.dataloader.tempout_loader_sqlite import (
    COL_TIME, COL_TEMP_OUT
)

"""
外気温集計モジュール by pandas
"""


@dataclass
class TempOut:
    appear_time: str
    temper: float


@dataclass
class TempOutStat:
    """ 外気温統計情報 """
    # 測定日
    measurement_day: str
    # 最低外気温情報
    min: TempOut
    # 最高外気温情報
    max: TempOut


def get_temp_out_stat(df_desc: DataFrame) -> TempOutStat:
    """ 外気温の統計情報 ([最低気温|最高気温] の気温とその出現時刻) を取得する """

    def get_measurement_time(pd_timestamp: pd.Timestamp) -> str:
        py_datetime: datetime = pd_timestamp.to_pydatetime()
        # 時刻部分は "時:分"までとする
        return py_datetime.strftime("%Y-%m-%d %H:%M")

    # 外気温列
    temp_out_ser: Series = df_desc[COL_TEMP_OUT]
    # 外気温列から最低・最高・平均気温を取得
    min_temper: np.float64 = temp_out_ser.min()
    max_temper: np.float64 = temp_out_ser.max()
    # 全ての最低気温を取得する
    df_min_all: DataFrame = df_desc[temp_out_ser <= min_temper]
    # 全ての最高気温を取得する
    df_max_all: pd.DataFrame = df_desc[temp_out_ser >= max_temper]
    # それぞれ直近の１レコードのみ取得
    min_first: Series = df_min_all.iloc[0]
    max_first: Series = df_max_all.iloc[0]
    # 測定日は先頭 10桁分(年月日)
    min_measurement_time: str = get_measurement_time(min_first[COL_TIME])
    measurement_day: str = min_measurement_time[:10]
    # 最低気温情報
    min_data: TempOut = TempOut(min_measurement_time, float(min_first[COL_TEMP_OUT]))
    # 最高気温情報
    max_measurement_time: str = get_measurement_time(max_first[COL_TIME])
    max_data: TempOut = TempOut(max_measurement_time, float(max_first[COL_TEMP_OUT]))
    return TempOutStat(measurement_day, min=min_data, max=max_data)
