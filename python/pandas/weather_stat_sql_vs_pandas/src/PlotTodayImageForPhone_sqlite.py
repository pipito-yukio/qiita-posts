import argparse
import os
import sqlite3

from typing import List, Optional

from pandas.core.frame import DataFrame

from plot_weather.dataloader.tempout_loader_sqlite import (
    get_dataframe
)
from plot_weather.plotter.plotterweather_sqlite import gen_plot_image
from batch_common import (
    get_connection, date_add_days, save_html
)

"""
気象センサーデータの当日データと前日データのプロット画像を取得する
スマートフォン版 ※描画領域サイズ必須
[DB] sqlite3 気象データ
"""

# スクリプト名
script_name = os.path.basename(__file__)

# 画層出力HTMLテンプレート
OUT_HTML = """
<!DOCTYPE html>
<html lang="ja">
<body>
<img src="{}"/>
</body>
</html>
"""


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
    # スマートフォンの描画領域サイズ ※任意
    parser.add_argument("--phone-image-size", type=str, required=False,
                        help="スマートフォンの描画領域サイズ['幅,高さ,密度'] (例) '1064x1704x2.75'")
    args: argparse.Namespace = parser.parse_args()
    # SQLite3 気象データペースファイルパス
    db_full_path: str = os.path.expanduser(args.db_path)
    # デバイス名
    device_name: str = args.device_name
    # 検索日
    find_date: str = args.find_date
    # スマホに表示するイメージビューのサイズ
    phone_size: str = args.phone_image_size

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection(db_full_path)
        # 検索日の観測データのDataFrame取得
        exclude_to_date: str = date_add_days(find_date)
        df_find: DataFrame = get_dataframe(conn, device_name, find_date, exclude_to_date)
        # 前日の観測データのDataFrame取得
        before_date: str = date_add_days(find_date, add_days=-1)
        df_before: DataFrame = get_dataframe(conn, device_name, before_date, find_date)
        if df_find.shape[0] > 0 and df_before.shape[0] > 0:
            # 画像取得
            html_img_src: str = gen_plot_image(df_find, df_before, phone_size=phone_size)
            # プロット結果をPNG形式でファイル保存
            script_names: List[str] = script_name.split(".")
            save_name = f"{script_names[0]}.html"
            save_path = os.path.join("output", save_name)
            print(save_path)
            html: str = OUT_HTML.format(html_img_src)
            save_html(save_path, html)
        else:
            print("該当レコードなし")
    except sqlite3.Error as db_err:
        print(f"type({type(db_err)}): {db_err}")
        exit(1)
    except Exception as exp:
        print(exp)
        exit(1)
    finally:
        if conn is not None:
            conn.close()
