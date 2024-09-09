import argparse
import csv
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

from db import pgdatabase

"""
Qiita投稿用スクリプト
不正アクセス集計CSVファイルを読み込み2つのテーブルに一括登録する
[スキーマ] mainte2
[テーブル]
  (1) 不正アクセスIPアドレステーブル
     unauth_ip_addr
  (2) 不正アクセスエラーカウントテーブル
     ssh_auth_error
"""

# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")


@dataclass(frozen=True)
class RegUnauthIpAddr:
    ip_addr: str
    reg_date: str


@dataclass(frozen=True)
class UnauthIpAddr:
    id: int
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
        csv_lines = [",".join(rec) for rec in reader]
    return csv_lines


def bulk_exists_ip_addr(conn: connection,
                        ip_list: List[str],
                        logger: Optional[logging.Logger] = None) -> Dict[str, int]:
    # IN ( in_clause )管理
    in_clause: Tuple[str, ...] = tuple(ip_list)
    if logger is not None:
        logger.debug(f"in_clause: {in_clause}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.execute("""
SELECT id,ip_addr FROM mainte2.unauth_ip_addr WHERE ip_addr IN %s""",
                        (in_clause,)
                        )
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
            # IN句で一致したIPアドレスの idとIPアドレスのタプルをすべて取得
            rows: List[tuple[Any, ...]] = cur.fetchall()
            if logger is not None:
                logger.debug(f"rows: {rows}")

            # 戻り値: IPアドレスをキーとするIPのIDの辞書
            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def bulk_insert_unauth_ip_addr(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[logging.Logger] = None) -> Dict[str, int]:
    if logger is not None:
        logger.debug(f"qry_params: \n{qry_params}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            rows: List[Tuple[Any, ...]] = execute_values(
                cur,
                """
INSERT INTO mainte2.unauth_ip_addr(ip_addr, reg_date)
 VALUES %s RETURNING id,ip_addr""",
                qry_params,
                template="(%(ip_addr)s, %(reg_date)s)",
                fetch=True
            )
            # 実行されたSQLを出力
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                logger.debug(f"rows: {rows}")

            # 戻り値: IPアドレスをキーとするIPのIDの辞書
            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


# ログ採取日のIP_IDリストが登録済みかチェックする
def bulk_exists_ssh_auth_error(
        conn: connection,
        log_date: str,
        ipid_list: List[int],
        logger: Optional[logging.Logger] = None) -> List[int]:
    # IN ( in_clause )
    in_clause: Tuple[int, ...] = tuple(ipid_list, )
    if logger is not None:
        logger.debug(f"in_clause: \n{in_clause}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.execute("""
SELECT
   ip_id
FROM
   mainte2.ssh_auth_error
WHERE
   log_date = %(log_date)s AND ip_id IN %(in_clause)s""",
                        {'log_date': log_date, 'in_clause': in_clause}
                        )
            # 実行されたSQLを出力
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                logger.debug(f"cur.rowcount: {cur.rowcount}")
            # 戻り値を取得する
            # def fetchall(self) -> list[tuple[Any, ...]]
            rows: List[Tuple[Any, ...]] = cur.fetchall()
            if logger is not None:
                logger.debug(f"rows: {rows}")

            # 結果が1カラムだけなのでタプルの先頭[0]をリストに格納
            result: List[int] = [row[0] for row in rows]
            return result
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def bulk_insert_ssh_auth_error(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[logging.Logger] = None) -> None:
    if logger is not None:
        logger.debug(f"qry_params: \n{qry_params}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            # 登録の戻り値不要
            execute_values(
                cur,
                """
INSERT INTO mainte2.ssh_auth_error(log_date, ip_id, appear_count)
 VALUES %s""",
                qry_params,
                template="(%(log_date)s, %(ip_id)s, %(appear_count)s)",
            )
            # 実行されたSQLを出力
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                # 登録済み処理件数 ※ログレベルをINFO
                logger.info(f"cur.rowcount: {cur.rowcount}")
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def get_register_ip_list(
        exists_ip_dict: Dict[str, int],
        csv_lines: List[str],
        logger: Optional[logging.Logger] = None) -> List[RegUnauthIpAddr]:
    result: List[RegUnauthIpAddr] = []
    if len(exists_ip_dict) > 0:
        registered_cnt: int = 0
        for line in csv_lines:
            fields: List[str] = line.split(",")
            registered_id: Optional[int] = exists_ip_dict.get(fields[1])
            if registered_id is None:
                result.append(RegUnauthIpAddr(
                    ip_addr=fields[1], reg_date=fields[0])
                )
            else:
                registered_cnt += 1
        if registered_cnt > 0:
            if logger is not None:
                logger.info(f"Registered_count: {registered_cnt}")
    else:
        # 登録済みレコードがない場合はすべて登録
        for line in csv_lines:
            fields = line.split(",")
            result.append(RegUnauthIpAddr(
                ip_addr=fields[1], reg_date=fields[0])
            )
    return result


def get_register_ssh_auth_error_list(
        exists_ip_dict: Dict[str, int],
        csv_lines: List[str],
        logger: Optional[logging.Logger] = None) -> List[SshAuthError]:
    result: List[SshAuthError] = []
    for line in csv_lines:
        fields: List[str] = line.split(",")
        ip_id: Optional[int] = exists_ip_dict.get(fields[1])
        if ip_id is not None:
            #  当該日のIPアドレスは不正アクセスIPアドレステーブルに登録済み
            result.append(
                SshAuthError(
                    log_date=fields[0], ip_id=ip_id, appear_count=int(fields[2])
                )
            )
        else:
            # このケースはない想定
            if logger is not None:
                logger.warning(f"{fields[1]} is not regstered!")
    return result


def insert_unauth_ip_main(
        conn: connection,
        exists_ip_dict: Dict[str, int],
        reg_ip_list: List[RegUnauthIpAddr],
        logger: Optional[logging.Logger] = None, enable_debug=False) -> None:
    # namedtupleを辞書のタプルに変換
    params: Tuple[Dict[str, Any], ...] = tuple([asdict(rec) for rec in reg_ip_list])
    registered_ip_ids: Dict[str, int] = bulk_insert_unauth_ip_addr(
        conn, params, logger=logger
    )
    if logger is not None:
        logger.info(f"registered_ip_ids.size: {len(registered_ip_ids)}")
        if logger is not None and enable_debug:
            logger.debug(f"registered_ip_ids: {registered_ip_ids}")
    # 新たに登録されたIPアドレスとIDを追加する
    exists_ip_dict.update(registered_ip_ids)
    if logger is not None and enable_debug:
        logger.debug(f"update.exists_ip_dict:\n{exists_ip_dict}")


def insert_ssh_auth_error_main(
        conn: connection,
        ssh_auth_error_list: List[SshAuthError],
        logger: Optional[logging.Logger] = None, enable_debug=False) -> None:
    # 当該日にIP_IDが登録済みかどうかチェックする ※誤って同一CSVを実行した場合を想定
    #  先頭レコードから当該日取得
    log_date: str = ssh_auth_error_list[0].log_date
    #  チェック用の ip_id リスト生成
    ipid_list: List[int] = [int(reg.ip_id) for reg in ssh_auth_error_list]
    exists_ipid_list: List[int] = bulk_exists_ssh_auth_error(
        conn, log_date, ipid_list, logger=logger if enable_debug else None
    )
    # 未登録の ip_id があれば登録レコード用のパラメータを生成
    if len(ipid_list) > len(exists_ipid_list):
        param_list: List[Any] = []
        for rec in ssh_auth_error_list:
            if rec.ip_id not in exists_ipid_list:
                # 当該日に未登録の ip_id のみのレコードの辞書オブジェクトを追加
                param_list.append(asdict(rec))
            else:
                if logger is not None and enable_debug:
                    logger.debug(f"Registered: {rec}")
        if len(param_list) > 0:
            if logger is not None and enable_debug:
                logger.debug(f"param_list: \n{param_list}")
            bulk_insert_ssh_auth_error(
                conn, tuple(param_list),
                logger=logger if enable_debug else None
            )
    else:
        if logger is not None:
            logger.info("ssh_auth_error テーブルに登録可能データなし.")


def batch_main():
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # レコード登録用CSVファイル: ~/Documents/webriverside/csv/ssh_auth_error_[日付].csv
    parser.add_argument("--csv-file", type=str, required=True,
                        help="Insert CSV file path.")
    parser.add_argument("--enable-debug", action="store_true",
                        help="Enable logger debug out.")
    args: argparse.Namespace = parser.parse_args()
    csv_file: str = args.csv_file
    enable_debug: bool = args.enable_debug
    if enable_debug:
        app_logger.setLevel(level=logging.DEBUG)
    else:
        app_logger.setLevel(level=logging.INFO)
    app_logger.info(f"csv-file: {csv_file}")

    # CSVファイルを開く
    csv_path = os.path.join(os.path.expanduser(csv_file))
    if not os.path.exists(csv_path):
        app_logger.error(f"FileNotFound: {csv_path}")
        exit(1)

    # CSVレコード: "log_date,ip_addr,appear_count"
    csv_lines: List[str] = read_csv(csv_path)
    line_cnt: int = len(csv_lines)
    # CSVファイル行数
    app_logger.info(f"csv: {line_cnt} lines.")
    if line_cnt == 0:
        app_logger.warning("Empty csv record.")
        exit(0)

    # database
    db: Optional[pgdatabase.PgDatabase] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE)
        conn: connection = db.get_connection()

        # CSVから取得したIPアドレス(2列目)が登録済みかチェック
        ip_list: List[str] = [line.split(",")[1] for line in csv_lines]
        exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(
            conn, ip_list, logger=app_logger)
        app_logger.info(f"exists_ip_dict.size: {len(exists_ip_dict)}")
        if enable_debug:
            app_logger.debug(f"exists_ip_dict: {exists_ip_dict}")

        # 登録済みIPアドレスを除外した追加登録用のレコードリストを作成
        reg_ip_datas: List[RegUnauthIpAddr] = get_register_ip_list(
            exists_ip_dict, csv_lines, logger=app_logger
        )

        # unauth_ip_addrテーブルとssh_auth_errorテーブル登録トランザクション
        reg_ip_datas_cnt: int = len(reg_ip_datas)
        app_logger.info(f"reg_ip_datas.size: {reg_ip_datas_cnt}")

        # 不正アクセスIPアドレステーブルに新規登録
        if reg_ip_datas_cnt > 0:
            insert_unauth_ip_main(
                conn, exists_ip_dict, reg_ip_datas,
                logger=app_logger, enable_debug=enable_debug
            )

        # 不正アクセスカウンターテーブル登録用リスト
        ssh_auth_error_list: List[SshAuthError] = get_register_ssh_auth_error_list(
            exists_ip_dict, csv_lines, logger=app_logger
        )
        app_logger.info(
            f"Register ssh_auth_error_list.size: {len(ssh_auth_error_list)}"
        )

        # 不正アクセスカウンターテーブルに新規
        if len(ssh_auth_error_list) > 0:
            insert_ssh_auth_error_main(
                conn, ssh_auth_error_list,
                logger=app_logger, enable_debug=enable_debug
            )
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


if __name__ == '__main__':
    batch_main()
