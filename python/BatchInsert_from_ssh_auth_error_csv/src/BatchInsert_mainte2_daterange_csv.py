import argparse
import csv
import glob
import logging
import os
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

from db import pgdatabase

"""
Qiita投稿用スクリプト
不正アクセスカウンターCSVファイルを読み込みし2つのテーブルに一括登録する
※1 Qiita投稿用テーブルは毎日CSVをインポートしていないためこのスクリプトを追加
※2 DEBUGログは出力しない
[スキーマ] mainte2
[テーブル]
  (1) 不正アクセスIPアドレステーブル
     unauth_ip_addr
  (2) 不正アクセスエラーカウントテーブル
     ssh_auth_error
"""


# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")
# CSVファイルディレクトリ
CSV_DIR: str = "~/Documents/webriverside/csv"


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


def check_date_range(from_date: str, to_date: str) -> bool:
    # 日付の大小チェック
    try:
        dt_from: date = date.fromisoformat(from_date)
        dt_to: date = date.fromisoformat(to_date)
        return dt_from <= dt_to
    except ValueError:
        return False


def extract_date_range(csv_files: List[str], from_date: str, to_date: str) -> List[str]:
    result: List[str] = []
    match_first: bool = False
    for f_name in csv_files:
        f_fields: Tuple[Any, Any] = os.path.splitext(f_name)
        f_name_only: str = f_fields[0]
        # to_date が含まれたCSVファイルなら追加して終了
        if match_first and f_name_only.endswith(to_date):
            result.append(f_name)
            break

        # 開始日が含まれたら開始フラグをTrueに設定
        if f_name_only.endswith(from_date):
            match_first = True

        # 開始フラグをTrueならファイル追加
        if match_first:
            result.append(f_name)

    return result


def bulk_exists_ip_addr(conn: connection,
                        ip_list: List[str],
                        logger: Optional[logging.Logger] = None) -> Dict[str, int]:
    # IN ( in_clause )
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


def csv_lines_insert(conn: connection,
                     csv_lines: List[str],
                     logger: Optional[logging.Logger] = None,
                     enable_debug: bool = False) -> None:
    try:
        # CSVから取得したIPアドレス(2列目)が登録済みかチェック
        ip_list: List[str] = [line.split(",")[1] for line in csv_lines]
        exists_ip_dict: Dict[str, int] = bulk_exists_ip_addr(conn, ip_list, logger=None)
        if logger is not None:
            logger.info(f"exists_ip_dict.size: {len(exists_ip_dict)}")

        # 登録済みIPアドレスを除外した追加登録用のレコードリストを作成
        reg_ip_datas: List[RegUnauthIpAddr] = get_register_ip_list(
            exists_ip_dict, csv_lines, logger=logger
        )

        # unauth_ip_addrテーブルとssh_auth_errorテーブル登録トランザクション
        reg_ip_datas_cnt: int = len(reg_ip_datas)
        if logger is not None:
            logger.info(f"reg_ip_datas.size: {reg_ip_datas_cnt}")

        # 不正アクセスIPアドレステーブル新規登録
        if reg_ip_datas_cnt > 0:
            insert_unauth_ip_main(
                conn, exists_ip_dict, reg_ip_datas,
                logger=logger, enable_debug=enable_debug
            )

        # 不正アクセスカウンターテーブル登録用リスト
        ssh_auth_error_list: List[SshAuthError] = get_register_ssh_auth_error_list(
            exists_ip_dict, csv_lines, logger=logger
        )
        if logger is not None:
            logger.info(
                f"Register ssh_auth_error_list.size: {len(ssh_auth_error_list)}"
            )

        # 不正アクセスカウンターテーブルに新規
        if len(ssh_auth_error_list) > 0:
            insert_ssh_auth_error_main(
                conn, ssh_auth_error_list,
                logger=logger, enable_debug=enable_debug
            )
    except psycopg2.Error:
        raise
    except Exception:
        raise


def batch_main():
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.INFO)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # レコード登録用CSVファイルの処理開始日付
    parser.add_argument("--from-date", type=str, required=True,
                        help="CSV file from date.")
    # レコード登録用CSVファイルの処理終了日付
    parser.add_argument("--to-date", type=str, required=True,
                        help="CSV file to date.")
    args: argparse.Namespace = parser.parse_args()
    # CSVファイルの開始日
    from_date: str = args.from_date
    to_date: str = args.to_date
    # 終了日が未指定なら当日
    if to_date is None:
        to_date = date.today().isoformat()

    app_logger.info(f"from-date: {from_date}, to-date: {to_date}")
    if not check_date_range(from_date, to_date):
        app_logger.error(
            f"Invalid date range: from_date={from_date}, to_date={to_date}"
        )
        exit(1)

    # CSVディレクトリ内のファイルリスト
    csv_dir_path: str = os.path.expanduser(CSV_DIR)
    # 指定したディレクトリ内の全てのCSVファイル ※取得したファイルはソートされていない
    csv_files: List[str] = glob.glob(os.path.join(csv_dir_path, "ssh_auth_error_*.csv"))
    csv_files = sorted(csv_files)
    match_files: List[str] = extract_date_range(csv_files, from_date, to_date)
    if len(match_files) == 0:
        app_logger.warning(f"Not match ({from_date} 〜 {to_date}) csv files.")
        exit(0)

    app_logger.debug(match_files)

    # database
    db: Optional[pgdatabase.PgDatabase] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, logger=None)
        conn: connection = db.get_connection()
        for filename in match_files:
            csv_lines: List[str] = read_csv(filename)
            csv_line: int = len(csv_lines)
            app_logger.info(f"{filename}: {csv_line}")
            if len(csv_lines) > 0:
                csv_lines_insert(conn, csv_lines, logger=app_logger)
        # 正常終了したらコミット
        db.commit()
    except psycopg2.Error as err:
        if db is not None:
            db.rollback()
        app_logger.error(err)
    except Exception as err:
        if db is not None:
            db.rollback()
        app_logger.error(err)
    finally:
        if db is not None:
            db.close()    


if __name__ == '__main__':
    batch_main()
