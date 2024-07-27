from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

"""
【Qiita投稿用】
レコードをINSERTした後に採番されるIDが必要なテーブルの操作関数を集めたモジュール
[対象テーブル] mainte2.unauth_ip_addr
"""


def bulk_exists_ip_addr(conn: connection,
                        ip_list: List[str],
                        logger: Optional[Logger] = None) -> Dict[str, int]:
    # IN ( in_clause )
    in_clause: Tuple[str, ...] = tuple(ip_list)
    if logger is not None:
        logger.debug(f"in_clause: {in_clause}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.execute("""
SELECT
  id,ip_addr
FROM mainte2.unauth_ip_addr
  WHERE ip_addr IN %s""",
                        (in_clause,)
                        )
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                logger.debug(f"cur.rowcount: {cur.rowcount}")
            # IN句で一致したIPアドレスの idとIPアドレスのタプルをすべて取得
            rows: List[tuple[Any, ...]] = cur.fetchall()
            if logger is not None:
                logger.debug(f"rows: {rows}")

            # 戻り値: IPアドレスをキーとするIPのIDの辞書
            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


# "VALUES" に続く (%s,%s),(%s,),... 自前で生成し、cur.execute()を実行する
def bulk_insert_rows_with_fetch(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[Logger] = None) -> Dict[str, int]:
    if logger is not None:
        logger.debug(f"qry_params: \n{qry_params}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            args: str = ",".join(
                # curr.mogrify()はバインド後のパラメータの部分SQL生成 ※返却されるのはバイト
                #  The returned string is always a bytes string.
                # 見やすくするために改行("\n")を入れていますが実際には不要
                [cur.mogrify("(%(ip_addr)s, %(reg_date)s)", param).decode('utf-8') + "\n"
                 for param in qry_params
                 ]
            )
            # 末尾の改行1文字を取り除いています: {args[:-1]} ※通常は {args}
            cur.execute(f"""
INSERT INTO mainte2.unauth_ip_addr(ip_addr, reg_date)
 VALUES {args[:-1]} RETURNING id,ip_addr""")
            # 実行されたSQLを出力
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                # 登録済み処理件数 ※ログレベルをINFO
                logger.info(f"cur.rowcount: {cur.rowcount}")
            # 戻り値を取得する
            rows: List[Tuple[Any, ...]] = cur.fetchall()
            if logger is not None:
                logger.debug(f"rows: {rows}")

            # 戻り値: IPアドレスをキーとするIPのIDの辞書
            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


# execute_values()メソッドを実行する ※内部で (%s,%s),(%s,),... 部分を生成してくれる
def bulk_insert_values_with_fetch(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[Logger] = None) -> Dict[str, int]:
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
                # 登録済み処理件数 ※ログレベルをINFO
                logger.info(f"cur.rowcount: {cur.rowcount}")
                logger.debug(f"rows: {rows}")

            # 戻り値: IPアドレスをキーとするIPのIDの辞書
            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def bulk_insert_many_with_fetch(
        conn: connection,
        qry_params: tuple[Dict[str, Any], ...],
        logger: Optional[Logger] = None) -> Dict[str, int]:
    if logger is not None:
        logger.debug(f"qry_params: \n{qry_params}")
    try:
        cur: cursor
        with conn.cursor() as cur:
            cur.executemany("""
INSERT INTO mainte2.unauth_ip_addr(ip_addr, reg_date)
 VALUES (%(ip_addr)s, %(reg_date)s)""",
                            qry_params
                            )
            # executemany()では最後に実行されたクエリーのみが出力される
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                # 登録済み処理件数 ※ログレベルをINFO
                logger.info(f"cur.rowcount: {cur.rowcount}")
            # 戻り値を取得する
            #  クエリーパラメータからIPアドレスのみ取り出す
            ip_list: List[str] = [param['ip_addr'] for param in qry_params]
            in_clause: Tuple[str, ...] = tuple(ip_list)
            cur.execute("""
SELECT
  id,ip_addr
FROM mainte2.unauth_ip_addr
  WHERE ip_addr IN %s""",
                        (in_clause,)
                        )
            rows: List[Tuple[Any, ...]] = cur.fetchall()
            if logger is not None:
                if cur.query is not None:
                    logger.debug(f"{cur.query.decode('utf-8')}")
                logger.debug(f"cur.rowcount: {cur.rowcount}")
                logger.debug(f"rows: {rows}")

            # 戻り値: IPアドレスをキーとするIPのIDの辞書
            result_dict: Dict[str, int] = {ip_addr: ip_id for (ip_id, ip_addr) in rows}
            return result_dict
    except (Exception, psycopg2.DatabaseError) as err:
        raise err
