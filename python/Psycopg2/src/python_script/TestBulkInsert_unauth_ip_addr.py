import argparse
import csv
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extensions import connection

from db import pgdatabase
# unauth_ip_addr テーブル操作関数
from dao.unauth_ip_addr import (
    bulk_exists_ip_addr,
    bulk_insert_many_with_fetch, bulk_insert_values_with_fetch
)

"""
Qiita投稿用: psycopg2ライブラリを使用したバッチ登録用テストスクリプト
1. 登録したレコードのIDを取得する必要があるクエリーのバッチ登録処理
[スキーマ] mainte2
[テーブル] unauth_ip_addr
[実行環境] Ubuntu
"""


# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")


@dataclass(frozen=True)
class RegUnauthIpAddr:
    ip_addr: str
    reg_date: str


def read_csv(file_name: str,
             skip_header=True, header_cnt=1) -> List[str]:
    with open(file_name, 'r') as fp:
        reader = csv.reader(fp, dialect='unix')
        if skip_header:
            for skip in range(header_cnt):
                next(reader)
        # リストをカンマ区切りで連結する
        lines = [",".join(rec) for rec in reader]
    return lines


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # CSVファイル: csv/xxxx_YYYY-mm-dd.csv
    parser.add_argument("--csv-file", type=str, required=True,
                        help="CSV file path.")
    # insert_type: "values" or "many"
    parser.add_argument("--insert-type", type=str,
                        choices=['many', 'values'],
                        default='values',
                        help="Insert type: ['many'|'values'(default)].")
    args: argparse.Namespace = parser.parse_args()
    app_logger.info(args)

    # CSVファイルチェック
    csv_file: str = args.csv_file
    csv_path: str
    if csv_file[0] == "~":
        # ユーザーディレクトリ
        csv_path = os.path.expanduser(csv_file)
    else:
        csv_path = csv_file
    if not os.path.exists(csv_path):
        app_logger.error(f"FileNotFound: {csv_path}")
        exit(1)

    # CSVファイル読み込み
    csv_lines: List[str] = read_csv(csv_path)
    line_size: int = len(csv_lines)
    app_logger.info(f"csv line.size: {line_size}")
    if line_size == 0:
        app_logger.info("Empty csv record.")
        exit(0)

    insert_type: str = args.insert_type
    # database
    db: Optional[pgdatabase.PgDatabase] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, logger=app_logger)
        conn: connection = db.get_connection()

        # CSVから取得したIPアドレス(1列目)が登録済みかチェック
        ip_list: List[str] = [csv_line.split(",")[1] for csv_line in csv_lines]
        exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(
            conn, ip_list, logger=app_logger
        )
        app_logger.info(f"exists_ip_dict: {exists_ip_dict}")

        # 登録済みIPアドレスを除外した追加登録用のレコードリストを作成
        fields: List[str]
        reg_datas: List[RegUnauthIpAddr] = []
        if len(exists_ip_dict) > 0:
            for csv_line in csv_lines:
                fields = csv_line.split(",")
                key_ip: str = fields[1]
                registered_id: Optional[int] = exists_ip_dict.get(key_ip)
                if registered_id is None:
                    reg_datas.append(
                        RegUnauthIpAddr(ip_addr=key_ip, reg_date=fields[0])
                    )
                else:
                    app_logger.info(f"Registered: {key_ip}")
        else:
            # 登録済みレコードがない場合はすべて登録 ※初回登録時
            reg_datas = [
                RegUnauthIpAddr(
                    ip_addr=csv_line.split(",")[1], reg_date=csv_line.split(",")[0]
                )
                for csv_line in csv_lines
            ]

        # 追加登録用のレコードリストがあれば登録する
        ret_ids: Dict[str, int]
        if len(reg_datas) > 0:
            # dataclassを辞書のタプルに変換
            params: Tuple[Dict[str, Any], ...] = tuple(
                [dict(asdict(rec)) for rec in reg_datas]
            )
            if insert_type == "many":
                ret_ids = bulk_insert_many_with_fetch(conn, params, logger=app_logger)
            else:
                ret_ids = bulk_insert_values_with_fetch(conn, params, logger=app_logger)
            app_logger.info(f"registered ids: {ret_ids}")
        else:
            app_logger.info("No registered record.")
        db.commit()
    except Exception as exp:
        app_logger.error(exp)
        if db is not None:
            db.rollback()
        exit(1)
    finally:
        if db is not None:
            db.close()    
