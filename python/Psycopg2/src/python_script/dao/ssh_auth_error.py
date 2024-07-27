from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values, execute_batch

"""
一括登録で戻り値が不要なテーブル操作モジュール
[テーブル] ssh_auth_error 
"""

"""
https://www.psycopg.org/docs/usage.html
  Tuples adaptation
  
  Python tuples are converted into a syntax suitable for the SQL IN operator and 
  to represent a composite type:
  ```python
  >>> cur.mogrify("SELECT %s IN %s;", (10, (10, 20, 30)))
  'SELECT 10 IN (10, 20, 30);'
  ```
"""


# ログ採取日のIPアドレスリストが登録済みかチェックする
def bulk_exists_logdate_with_ipaddr(
        conn: connection,
        log_date: str,
        ip_list: List[str],
        logger: Optional[Logger] = None) -> Dict[str, int]:
    # IN ( in_clause )
    in_clause: Tuple[str, ...] = tuple(ip_list, )
    if logger is not None:
        logger.debug(f"in_clause: \n{in_clause}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.execute("""
SELECT
   ip_addr, ip_id
FROM
   mainte2.ssh_auth_error sae
   INNER JOIN mainte2.unauth_ip_addr uia ON uia.id = sae.ip_id
WHERE
   log_date = %(log_date)s AND ip_addr IN %(in_clause)s""",
                        {'log_date': log_date, 'in_clause': in_clause}
                        )
            # 実行されたSQLを出力
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
            # 戻り値を取得する
            # def fetchall(self) -> list[tuple[Any, ...]]
            rows: List[Tuple[Any, ...]] = cur.fetchall()
            if logger is not None:
                logger.debug(f"rows: {rows}")

            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


# ログ採取日のIP_IDリストが登録済みかチェックする
def bulk_exists_logdate_with_ipid(
        conn: connection,
        log_date: str,
        ipid_list: List[int],
        logger: Optional[Logger] = None) -> List[int]:
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


def bulk_insert_batch(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[Logger] = None) -> None:
    if logger is not None:
        logger.debug(f"qry_params: \n{qry_params}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            # 戻り値を返さない関数
            execute_batch(
                cur,
                """
INSERT INTO mainte2.ssh_auth_error(log_date, ip_id, appear_count)
 VALUES (%(log_date)s, %(ip_id)s, %(appear_count)s)""",
                qry_params)
            # 実行されたSQLを出力
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                # 登録済み処理件数 ※ログレベルをINFO
                logger.info(f"cur.rowcount: {cur.rowcount}")
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def bulk_insert_values(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[Logger] = None) -> None:
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


def bulk_insert_many(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[Logger] = None) -> None:
    if logger is not None:
        logger.debug(f"qry_params: \n{qry_params}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.executemany("""
INSERT INTO mainte2.ssh_auth_error(log_date, ip_id, appear_count)
 VALUES (%(log_date)s, %(ip_id)s, %(appear_count)s)""",
                            qry_params
                            )
            # executemany()では最後に実行されたクエリーのみが出力される
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                # 登録済み処理件数 ※ログレベルをINFO
                logger.info(f"cur.rowcount: {cur.rowcount}")
    except (Exception, psycopg2.DatabaseError) as err:
        raise err
