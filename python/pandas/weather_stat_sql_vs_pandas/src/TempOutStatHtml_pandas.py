import argparse
import os
import sqlite3
from typing import Dict, List, Optional

from pandas.core.frame import DataFrame

from plot_weather.dataloader.tempout_loader_sqlite import (
    get_dataframe
)
from plot_weather.dataloader.tempout_stat import (
    get_temp_out_stat, TempOutStat, TempOut
)
from batch_common import (
    get_connection, date_add_days, to_title_date, save_html, OUT_HTML
)

"""
外気温の当日データと前日データの統計情報をHTMLに出力
[DB] sqlite3 気象データ
[集計方法] pandas
"""

# スクリプト名
script_name = os.path.basename(__file__)


if __name__ == '__main__':
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # データペースパス: ~/db/weather.db
    parser.add_argument("--db-path", type=str, required=True,
                        help="SQLite3 Database path.")
    # デバイス名: esp8266_1
    parser.add_argument("--device-name", type=str, required=True,
                        help="device name in t_device.")
    # 検索日: 2023-11-01
    parser.add_argument("--find-date", type=str, required=True,
                        help="ISO8601 format.")
    args: argparse.Namespace = parser.parse_args()
    # SQLite3 気象データペースファイルパス
    db_full_path: str = os.path.expanduser(args.db_path)
    # デバイス名
    device_name: str = args.device_name
    # 検索日
    find_date: str = args.find_date

    html_dict: Dict = {}
    exclude_to_date: str
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection(db_full_path)
        # 指定日の翌日 (含まない)
        exclude_to_date = date_add_days(find_date)
        df_find: DataFrame = get_dataframe(conn, device_name, find_date, exclude_to_date)
        # 外気温統計データ
        find_stat: TempOutStat = get_temp_out_stat(df_find)
        find_day_min: TempOut = find_stat.min
        find_day_max: TempOut = find_stat.max
        print(f"today_min: {find_day_min}, today_max: {find_day_max}")
        # HTML用辞書オブジェクトに指定日データを設定する
        html_dict["find_day"] = to_title_date(find_date)
        html_dict["find_min_time"] = find_day_min.appear_time[11:16]
        html_dict["find_min_temper"] = find_day_min.temper
        html_dict["find_max_time"] = find_day_max.appear_time[11:16]
        html_dict["find_max_temper"] = find_day_max.temper

        # 前日の統計情報
        before_date: str = date_add_days(find_date, add_days=-1)
        df_before: DataFrame = get_dataframe(conn, device_name, before_date, find_date)
        before_stat: TempOutStat = get_temp_out_stat(df_before)
        before_day_min: TempOut = before_stat.min
        before_day_max: TempOut = before_stat.max
        print(f"before_min: {before_day_min}, before_max: {before_day_max}")
        # HTML用辞書オブジェクトに前日データを設定する
        html_dict["before_day"] = to_title_date(before_date)
        html_dict["before_min_time"] = before_day_min.appear_time[11:16]
        html_dict["before_min_temper"] = before_day_min.temper
        html_dict["before_max_time"] = before_day_max.appear_time[11:16]
        html_dict["before_max_temper"] = before_day_max.temper
        print(html_dict)

        # HTMLを生成する
        html: str = OUT_HTML.format(**html_dict)
        script_names: List[str] = script_name.split(".")
        save_name = f"{script_names[0]}.html"
        save_path = os.path.join("output", save_name)
        print(save_path)
        save_html(save_path, html)
    except sqlite3.Error as db_err:
        print(f"type({type(db_err)}): {db_err}")
        exit(1)
    except Exception as exp:
        print(exp)
        exit(1)
    finally:
        if conn is not None:
            conn.close()
