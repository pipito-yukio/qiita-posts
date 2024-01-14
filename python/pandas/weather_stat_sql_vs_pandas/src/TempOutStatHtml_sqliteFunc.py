import argparse
import os
import sqlite3
from typing import Dict, List, Optional

from plot_weather.dao.windowfunc_sqlite import (
    get_temp_out_stat, TempOut
)
from batch_common import (
    get_connection, date_add_days, to_title_date, save_html, OUT_HTML
)

"""
外気温の当日データと前日データの統計情報をHTMLに出力
[DB] sqlite3 気象データ
[集計方法] SQL Window function
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
        # 検索日のデータ
        exclude_to_date = date_add_days(find_date)
        stat_data: List[TempOut] = get_temp_out_stat(
            conn, device_name, find_date, exclude_to_date
        )
        find_day_min: TempOut = stat_data[0]
        find_day_max: TempOut = stat_data[1]
        print(f"today_min: {find_day_min}, today_max: {find_day_max}")
        # HTML用辞書オブジェクトに指定日データを設定する
        html_dict["find_day"] = to_title_date(find_date)
        html_dict["find_min_time"] = find_day_min.appear_time[11:16]
        html_dict["find_min_temper"] = find_day_min.temper
        html_dict["find_max_time"] = find_day_max.appear_time[11:16]
        html_dict["find_max_temper"] = find_day_max.temper

        # 前日の統計情報
        before_date: str = date_add_days(find_date, add_days=-1)
        stat_data: List[TempOut] = get_temp_out_stat(
            conn, device_name, before_date, find_date
        )
        before_day_min: TempOut = stat_data[0]
        before_day_max: TempOut = stat_data[1]
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
