import argparse
import csv
import logging
import os
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extensions import connection

from db import pgdatabase
from dao.unauth_ip_addr import (
    bulk_exists_ip_addr, 
    bulk_insert_values_with_fetch
)

"""
Qiita投稿用: 指定したディレクトリ内の複数のCSVファイルから一括登録する
"""


# ログフォーマット
LOG_FMT: str = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
LOG_DATE_FMT: str = '%Y-%m-%d %H:%M:%S'
# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")

# 有効なCSVファイルパターン
CSV_PATTERN: re.Pattern = re.compile(r'^ssh_auth_error_\d{4}-\d{2}-\d{2}.csv$')


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
    logging.basicConfig(format=LOG_FMT, datefmt=LOG_DATE_FMT)
    app_logger = logging.getLogger(__name__)
    # 大量データの登録のためINFOのみ出力
    app_logger.setLevel(level=logging.INFO)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # CSVファイル格納ディレクトリ
    parser.add_argument("--csv-dir", type=str, required=True,
                        help="CSV files directory.")
    # 読み込むファイル数 ※任意
    parser.add_argument("--file-limit", type=int,
                        help="処理する CSVファイル数 ※未指定なら指定されたディレクトリのすべてのファイル")
    # ホスト名: 任意 (例) hp-z820 ※末尾に ".local"はつけない
    parser.add_argument("--db-host", type=str, help="Other database hostname.")
    args: argparse.Namespace = parser.parse_args()
    # 処理するファイル数
    file_limit: Optional[int] = args.file_limit
    # DBサーバーホスト
    db_host = args.db_host

    csv_dir: str = args.csv_dir
    csv_base_path: str
    if csv_dir[0] == "~":
        csv_base_path = os.path.expanduser(args.csv_dir)
    else:
        csv_base_path = csv_dir
    if not os.path.exists(csv_base_path):
        app_logger.error(f"{csv_base_path} not found!")
        exit(1)

    # 指定されたディレクトリのすべてのファイル
    csv_names: List[str] = os.listdir(csv_base_path)
    # ファイル名でソートする
    csv_names.sort()
    csv_files: List[str] = []
    # 処理するCSVファイル数
    proc_count: int = 0
    for csv_file in csv_names:
        if file_limit is None or proc_count < file_limit:
            mat: Optional[re.Match] = CSV_PATTERN.search(csv_file)
            if mat:
                csv_files.append(os.path.join(csv_base_path, csv_file))
                proc_count += 1
    app_logger.info(f"csv_files: {len(csv_files)}")

    # database
    db: Optional[pgdatabase.PgDatabase] = None
    conn: Optional[connection] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, hostname=db_host, logger=app_logger)
        conn = db.get_connection()

        # 指定されたディレクトリ内の該当するCSVファイル
        for csv_file in csv_files:
            csv_lines: List[str] = read_csv(csv_file)
            app_logger.info(f"{os.path.basename(csv_file)}: {len(csv_lines)} lines")
            # CSVから取得したIPアドレス(2列目)が登録済みかチェック
            ip_list: List[str] = [csv_line.split(",")[1] for csv_line in csv_lines]
            exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(
                conn, ip_list, logger=None
            )
            app_logger.info(f"exists_ip_dict.size: {len(exists_ip_dict)}")

            # 登録済みIPアドレスを除外した追加登録用のレコードリストを作成
            reg_datas: List[RegUnauthIpAddr] = []
            fields: List[str]
            if len(exists_ip_dict) > 0:
                registered_cnt: int = 0
                for csv_line in csv_lines:
                    fields = csv_line.split(",")
                    key: str = fields[1]
                    registered_id: Optional[int] = exists_ip_dict.get(key)
                    if registered_id is None:
                        reg_datas.append(
                            RegUnauthIpAddr(ip_addr=key, reg_date=fields[0])
                        )
                    else:
                        registered_cnt += 1
                app_logger.info(f"Registered.count: {registered_cnt}")
            else:
                # 登録済みレコードがない場合はすべて登録
                for csv_line in csv_lines:
                    fields = csv_line.split(",")
                    reg_datas.append(
                        RegUnauthIpAddr(ip_addr=fields[1], reg_date=fields[0])
                    )

            # 追加登録用のレコードリストがあれば登録する
            ret_ids: Dict[str, int]
            if len(reg_datas) > 0:
                # dataclassを辞書のタプルに変換
                params: Tuple[Dict[str, Any], ...] = tuple(
                    [dict(asdict(rec)) for rec in reg_datas]
                )
                ret_ids = bulk_insert_values_with_fetch(
                    conn, params, logger=app_logger
                )
                app_logger.info(f"registered ids.size: {len(ret_ids)}")
            else:
                app_logger.info("No registered record.")

        conn.commit()
    except Exception as exp:
        if conn:
            conn.rollback()
        app_logger.error(exp)
        exit(1)
    finally:
        if db is not None:
            db.close()    
