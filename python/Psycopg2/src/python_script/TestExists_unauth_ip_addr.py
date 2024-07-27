import argparse
import csv
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extensions import connection

from db import pgdatabase
from dao.unauth_ip_addr import (
    bulk_exists_ip_addr, bulk_insert_values_with_fetch
)

"""
Qiita投稿用: IPアドレスの一括登録チェック
[スキーマ] mainte2
[テーブル] unauth_ip_addr
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
                        help="Insert CSV file path.")
    args: argparse.Namespace = parser.parse_args()
    # CSVファイルを開く
    csv_file: str = args.csv_file
    csv_path = os.path.join(os.path.expanduser(csv_file))
    if not os.path.exists(csv_path):
        app_logger.error(f"FileNotFound: {csv_path}")
        exit(1)

    # CSVレコード: "log_date,ip_addr,appear_count"
    csv_lines: List[str] = read_csv(csv_path)
    app_logger.info(csv_lines)
    if len(csv_lines) == 0:
        app_logger.info("Empty csv record.")
        exit(0)

    # database
    db: Optional[pgdatabase.PgDatabase] = None
    conn: Optional[connection] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, logger=app_logger)
        conn = db.get_connection()
        # CSVから取得したIPアドレス(2列目)が登録済みかチェック
        ip_list: List[str] = [csv.split(",")[1] for csv in csv_lines]
        exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(
            conn, ip_list, logger=app_logger)
        app_logger.info(f"exists_ip_dict: {exists_ip_dict}")

        # 登録済みIPアドレスを除外した追加登録用のレコードリストを作成
        fields: List[str]
        reg_ip_datas: List[RegUnauthIpAddr] = []
        if len(exists_ip_dict) > 0:
            for csv_line in csv_lines:
                fields = csv_line.split(",")
                registered_id: Optional[int] = exists_ip_dict.get(fields[1])
                if registered_id is None:
                    reg_ip_datas.append(RegUnauthIpAddr(
                        ip_addr=fields[1], reg_date=fields[0])
                    )
                else:
                    app_logger.info(f"Registered: {fields[1]}")
        else:
            # 登録済みレコードがない場合はすべて登録
            for csv_line in csv_lines:
                fields = csv_line.split(",")
                reg_ip_datas.append(RegUnauthIpAddr(
                    ip_addr=fields[1], reg_date=fields[0])
                )

        # 不正アクセスIPアドレステーブル新規登録
        if len(reg_ip_datas) > 0:
            # namedtupleを辞書のタプルに変換
            params: Tuple[Dict[str, Any], ...] = tuple(
                [dict(asdict(rec)) for rec in reg_ip_datas]
            )
            registered_ip_ids: Dict[str, int] = bulk_insert_values_with_fetch(
                conn, params, logger=app_logger
            )
            app_logger.info(f"registered_ip_ids ids: {registered_ip_ids}")
            # 新たに登録されたIPアドレスとIDを追加する
            exists_ip_dict.update(registered_ip_ids)
            app_logger.info(f"update.exists_ip_dict:\n{exists_ip_dict}")

            db.commit()
    except Exception as exp:
        if db is not None:
            db.rollback()
        app_logger.error(exp)
        exit(1)
    finally:
        if db is not None:
            db.close()    
