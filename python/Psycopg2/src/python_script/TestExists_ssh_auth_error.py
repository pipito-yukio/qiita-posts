import argparse
import csv
import logging
import os
from typing import Dict, List, Optional

from psycopg2.extensions import connection

from db import pgdatabase
from dao.ssh_auth_error import (
    bulk_exists_logdate_with_ipaddr
)

"""
Qiita投稿用: テーブルを結合したレコードの取得
[スキーマ] mainte2
[テーブル] 結合
　　unauth_ip_addr
   ssh_auth_error
"""


# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")


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
        # 先頭レコードからログ日付取得(1列目)
        log_date: str = csv_lines[0].split(",")[0]
        # ログ日付のIPアドレス(2列目)が登録済みかチェック
        ip_list: List[str] = [csv.split(",")[1] for csv in csv_lines]
        exists_dict: Dict[str, int] = bulk_exists_logdate_with_ipaddr(
            conn, log_date, ip_list, logger=app_logger
        )
        app_logger.info(f"exists_dict: {exists_dict}")

    except Exception as exp:
        app_logger.error(exp)
        exit(1)
    finally:
        if db is not None:
            db.close()    
