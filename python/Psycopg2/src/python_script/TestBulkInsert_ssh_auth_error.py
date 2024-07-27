import argparse
import csv
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extensions import connection

from db import pgdatabase
# unauth_ip_addr テーブル
from dao.unauth_ip_addr import (
    bulk_exists_ip_addr, bulk_insert_values_with_fetch
)
# ssh_auth_error テーブル
from dao.ssh_auth_error import (
    bulk_exists_logdate_with_ipid,
    bulk_insert_values, bulk_insert_batch, bulk_insert_many
)


"""
Qiita投稿用: psycopg2ライブラリを使用したバッチ登録用テストスクリプト
1. 1つのテーブルに一括登録する
[スキーマ] mainte2
[テーブル] 
   ssh_auth_error: 戻り値なしの一括登録
[レコード] dataclass
"""

# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")


@dataclass(frozen=True)
class RegUnauthIpAddr:
    ip_addr: str
    reg_date: str


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
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # CSVファイル: csv/xxxx_YYYY-mm-dd.csv
    parser.add_argument("--csv-file", type=str, required=True,
                        help="CSV file path.")
    # insert_type: "batch" or "many" or "value"(default)
    parser.add_argument("--insert-type", type=str,
                        choices=["batch", "many", "values"],
                        default="values",
                        help="Insert type: ['batch'|'many'|'values'(default)].")
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

        # CSVから取得したIPアドレス(2列目)が登録済みかチェック
        ip_list: List[str] = [csv.split(",")[1] for csv in csv_lines]
        exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(
            conn, ip_list, logger=app_logger
        )
        app_logger.info(f"exists_ip_dict: {exists_ip_dict}")

        # 登録済みIPアドレスを除外した追加登録用のレコードリストを作成
        key_ip: str
        fields: List[str]
        reg_unauth_ip_datas: List[RegUnauthIpAddr] = []
        if len(exists_ip_dict) > 0:
            # 登録済のIPアドレスに存在しなければ登録用データに追加する
            for csv_line in csv_lines:
                fields = csv_line.split(",")
                key_ip = fields[1]
                registered_id: Optional[int] = exists_ip_dict.get(key_ip)
                if registered_id is None:
                    # 未登録のため追加する
                    reg_unauth_ip_datas.append(
                        RegUnauthIpAddr(ip_addr=key_ip, reg_date=fields[0])
                    )
                else:
                    app_logger.info(f"Registered: {key_ip}")
        else:
            # 登録済みレコードがない場合はすべて登録 ※初回実行時のみ
            reg_datas = [
                RegUnauthIpAddr(ip_addr=csv.split(",")[1], reg_date=csv.split(",")[0])
                for csv in csv_lines
            ]

        # 新規登録データがあれば不正アクセスIPマスタに一括登録する
        # ※当該日のIPアドレスがすべて不正アクセスIPマスタに登録済のケースは有りうる
        if len(reg_unauth_ip_datas) > 0:
            # dataclassを辞書のタプルに変換
            params: Tuple[Dict[str, Any], ...] = tuple(
                [asdict(rec) for rec in reg_unauth_ip_datas]
            )
            # 不正アクセスIPアドレステーブル新規登録
            registered_ip_dict: Dict[str, int] = bulk_insert_values_with_fetch(
                conn, params, logger=app_logger
            )
            app_logger.info(f"registered_ip_dict.size: {len(registered_ip_dict)}")
            # 新たに一括登録されたIPアドレス-ID辞書を元のIPアドレス-ID辞書に追加する
            exists_ip_dict.update(registered_ip_dict)
            app_logger.info(f"Updated exists_ip_dict.size:{len(exists_ip_dict)}")
        # else:
        #  すべて登録済なので追加の登録処理なし

        # 不正アクセスカウンターテーブルへの一括登録用データリスト
        reg_ssh_auth_error_datas: List[SshAuthError] = []
        for csv_line in csv_lines:
            fields = csv_line.split(",")
            key_ip = fields[1]
            # ipアドレスから ip_id を取得する ※基本的にはすべてIDを取得できる想定
            ip_id: Optional[int] = exists_ip_dict.get(key_ip)
            if ip_id:
                reg_ssh_auth_error_datas.append(
                    SshAuthError(
                        log_date=fields[0], ip_id=ip_id, appear_count=int(fields[2])
                    )
                )
            else:
                # このケースはない想定
                app_logger.warning(f"{key_ip} is not registered!")

        app_logger.info(f"reg_ssh_auth_error_datas.size: {len(reg_ssh_auth_error_datas)}")
        # 当該日の ip_id が登録済みかどうかチェック
        # ※当該日のデータが追加されるケースを想定。ただし同一データなら追加登録データ無しになる
        if len(reg_ssh_auth_error_datas) > 0:
            #  先頭レコードから当該日取得
            log_date: str = reg_ssh_auth_error_datas[0].log_date
            #  登録済チェック用の ip_id リスト生成
            ipid_list: List[int] = [int(reg.ip_id) for reg in reg_ssh_auth_error_datas]
            exists_ipid_list: List[int] = bulk_exists_logdate_with_ipid(
                conn, log_date, ipid_list, logger=app_logger
            )
            # 未登録の ip_id があれば登録レコード用のパラメータを生成
            if len(ipid_list) > len(exists_ipid_list):
                rec_params: List[Any] = []
                for rec in reg_ssh_auth_error_datas:
                    if rec.ip_id not in exists_ipid_list:
                        # 当該日に未登録の ip_id のみのレコードの辞書オブジェクトを追加
                        rec_params.append(asdict(rec))
                    else:
                        app_logger.info(f"Registered: {rec}")

                if len(rec_params) > 0:
                    if insert_type == "many":
                        bulk_insert_many(conn, tuple(rec_params), logger=app_logger)
                    elif insert_type == "batch":
                        bulk_insert_batch(conn, tuple(rec_params), logger=app_logger)
                    else:
                        bulk_insert_values(conn, tuple(rec_params), logger=app_logger)
            else:
                app_logger.info(f"{log_date}: ssh_auth_errorテーブルに登録可能データなし")

        # 両方のテーブル登録で正常終了したらコミット
        db.commit()
    except Exception as exp:
        app_logger.error(exp)
        if db is not None:
            db.rollback()
        exit(1)
    finally:
        if db is not None:
            db.close()
