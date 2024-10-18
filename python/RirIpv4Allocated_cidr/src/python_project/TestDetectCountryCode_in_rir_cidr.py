import argparse
import json
import logging
import os
import socket

from logging import Logger
from typing import Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor

"""
Qiita投稿用スクリプト
IPアドレスのネットワークアドレスと国コードをRIRデータテーブルから検索する

[テーブル名] mainte2.RIR_ipv4_allocated_cidr
"""

# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")


class PgDatabase(object):
    def __init__(self, configfile,
                 hostname: Optional[str] = None,
                 logger: Optional[logging.Logger] = None):
        self.logger = logger
        with open(configfile, 'r') as fp:
            db_conf = json.load(fp)
            if hostname is None:
                hostname = socket.gethostname()
            db_conf["host"] = db_conf["host"].format(hostname=hostname)
        self.conn = psycopg2.connect(**db_conf)
        if self.logger is not None:
            self.logger.debug(self.conn)

    def get_connection(self) -> connection:
        return self.conn

    def rollback(self) -> None:
        if self.conn is not None:
            self.conn.rollback()

    def commit(self) -> None:
        if self.conn is not None:
            self.conn.commit()

    def close(self) -> None:
        if self.conn is not None:
            if self.logger is not None:
                self.logger.debug(f"Close {self.conn}")
            self.conn.close()


# ターゲットIPのネットワークがネットワークアドレス国コードテーブルに存在するかチェック
def target_ip_include_rir_table(
        conn: connection,
        target_ip: str,
        logger: Optional[Logger] = None) -> Optional[Tuple[str, str]]:
    if logger is not None:
        logger.debug(f"target_ip: {target_ip}")

    result: Optional[Tuple[str, str]]
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.execute("""
SELECT
  network_addr, country_code
FROM
  mainte2.RIR_ipv4_allocated_cidr
WHERE
  inet %(target_ip)s << network_addr""",
                        ({'target_ip': target_ip}))
            row: Optional[Tuple[str, str]] = cur.fetchone()
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                logger.debug(f"row: {row}")
            result = row
        return result
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def batch_main():
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("--target-ip", required=True, type=str,
                        help="IP address.")
    parser.add_argument("--enable-debug", action="store_true",
                        help="Enable logger debug out.")
    args: argparse.Namespace = parser.parse_args()
    target_ip: str = args.target_ip
    enable_debug: bool = args.enable_debug

    db: Optional[PgDatabase] = None
    try:
        db = PgDatabase(DB_CONF_FILE, logger=None)
        conn: connection = db.get_connection()
        # 地域インターネットレジストリマスタテーブル検索
        find_rec: Optional[Tuple[str, str]] = target_ip_include_rir_table(
            conn, target_ip, logger=app_logger if enable_debug else None
        )
        if find_rec is not None:
            network: str = find_rec[0]
            cc: str = find_rec[1]
            app_logger.info(
                f'Find {target_ip} in RIR_ipv4_allocated_cidr("{network}"'
                f', country_code: "{cc}")'
            )
        else:
            app_logger.info(f"{target_ip} is not match in tables.")
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        exit(1)
    except Exception as err:
        app_logger.error(err)
        exit(1)
    finally:
        if db is not None:
            db.close()


if __name__ == '__main__':
    batch_main()
