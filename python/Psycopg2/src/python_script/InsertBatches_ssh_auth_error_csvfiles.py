import argparse
import csv
import logging
import os
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from psycopg2.extensions import connection

from db import pgdatabase
from dao.unauth_ip_addr import bulk_exists_ip_addr
from dao.ssh_auth_error import (
    bulk_insert_values, bulk_insert_batch, bulk_insert_many
)

"""
Qiita投稿用: 指定したディレクトリ内の複数のCSVファイルから一括登録する
各バッチ関数によるパフォーマンス比較のためのスクリプト
"""

# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")

# 有効なCSVファイルパターン
CSV_PATTERN: re.Pattern = re.compile(r'^ssh_auth_error_\d{4}-\d{2}-\d{2}.csv$')


@dataclass(frozen=True)
class SshAuthError:
    log_date: str
    ip_id: int
    appear_count: int


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
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
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
    # 一括処理関数型:
    parser.add_argument("--insert-type", type=str,
                        choices=["batch", "many", "values"], default="values",
                        help="Bulk insert: 'batch'|'values'|'many', default 'values'.")
    # ホスト名: 任意 (例) hp-z820 ※末尾に ".local"はつけない
    parser.add_argument("--db-host", type=str, help="Other database hostname.")
    args: argparse.Namespace = parser.parse_args()
    app_logger.info(args)
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
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, hostname=db_host, logger=app_logger)
        conn: connection = db.get_connection()

        # 指定されたディレクトリ内の該当するCSVファイル
        registered_ip_dict: Dict = {}
        # 登録予定の一括登録用データリスト
        reg_datas: List[SshAuthError] = []
        for i, csv_file in enumerate(csv_files):
            csv_lines: List[str] = read_csv(csv_file)
            app_logger.info(f"{os.path.basename(csv_file)}: {len(csv_lines)} lines")
            # CSVから取得したIPアドレス(2列目)が登録済みかチェック ※ログを出力しない
            ip_list: List[str] = [csv_line.split(",")[1] for csv_line in csv_lines]
            exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(
                conn, ip_list, logger=None
            )
            # 登録済みIPアドレス辞書を蓄積する
            registered_ip_dict.update(exists_ip_dict)
            app_logger.info(f"registered_ip_dict[{i}].size: {len(registered_ip_dict)}")

            # 登録済みIPアドレスのみを一括登録データとする
            ip_addr: str
            ip_id: Optional[int]
            for csv_line in csv_lines:
                fields: List[str] = csv_line.split(",")
                ip_addr = fields[1]
                ip_id = registered_ip_dict.get(ip_addr)
                if ip_id is not None:
                    reg_datas.append(SshAuthError(
                        log_date=fields[0], ip_id=ip_id, appear_count=int(fields[2])
                        )
                    )

        app_logger.info(f"reg_ssh_auth_error.size: {len(reg_datas)}")

        # 一括処理関数
        insert_type: str = args.insert_type
        # ssh_auth_error テーブルは空を前提にしているのでチェック不要で一括登録
        if len(reg_datas) > 0:
            param_list: List[Any] = [asdict(rec) for rec in reg_datas]
            app_logger.info(f"Batch['{insert_type}'] START.")
            if insert_type == "many":
                bulk_insert_many(
                    conn, tuple(param_list), logger=app_logger
                )
            elif insert_type == "batch":
                bulk_insert_batch(
                    conn, tuple(param_list), logger=app_logger
                )
            else:
                bulk_insert_values(
                    conn, tuple(param_list), logger=app_logger
                )
        app_logger.info(f"Batch['{insert_type}'] END.")

        # 両方のテーブル登録で正常終了したらコミット
        db.commit()
    except Exception as exp:
        if db is not None:
            db.rollback()
        app_logger.error(exp)
        exit(1)
    finally:
        if db is not None:
            db.close()
