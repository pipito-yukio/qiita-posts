import argparse
import csv
import logging
import os
from dataclasses import dataclass, asdict
from ipaddress import (
    ip_address, summarize_address_range,
    IPv4Network, IPv4Address
)
import typing
from typing import Dict, List, Iterator, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor

from db import pgdatabase

"""
[Qiita投稿No38用スクリプト]
指定されたIPアドレスの属するネットワーク(CIDR表記)と国コードをRIRデータから取得する
"""

# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")


@dataclass(frozen=True)
class RirRecord:
    ip_start: str
    ip_count: int
    country_code: str


@typing.no_type_check
# Incompatible types in assignment (expression has type
#  "IPv4Address | IPv6Address", variable has type "IPv4Address")  [assignment]
#  See: https://mypy.readthedocs.io/en/stable/
#     type_inference_and_annotations.html#type-ignore-error-codes
def get_cidr_cc_list(ip_start: str,
                     ip_count: int,
                     country_code: str) -> List[Tuple[IPv4Network, str]]:
    # mypy check error IPv?Address to Any.
    # IP address: IPv6Address | IPv4Address
    addr_first: IPv4Address = ip_address(ip_start)
    # Broadcast address
    addr_last: IPv4Address = addr_first + ip_count - 1
    cidr_ite: Iterator[IPv4Network] = summarize_address_range(addr_first, addr_last)
    return [(cidr, country_code) for cidr in cidr_ite]


@typing.no_type_check
def detect_cc_in_cidr_cc_list(
        target_ip: str,
        cidr_cc_list: List[Tuple[IPv4Network, str]]
        ) -> Tuple[Optional[str], Optional[str]]:
    target_ip_addr: IPv4Address = ip_address(target_ip)
    match_network: Optional[str] = None
    match_cc: Optional[str] = None
    for cidr_cc in cidr_cc_list:
        if target_ip_addr in cidr_cc[0]:
            match_network = str(cidr_cc[0])
            match_cc = cidr_cc[1]
            break

    return match_network, match_cc


def get_rir_table_matches(
        conn: connection,
        like_ip: str,
        logger: Optional[logging.Logger] = None) -> List[Tuple[str, int, str]]:
    if logger is not None:
        logger.debug(f"like_ip: {like_ip}")
    result: List[Tuple[str, int, str]]
    try:
        cur: cursor
        # 前ゼロ埋めしたIPアドレスの昇順にソートする
        with conn.cursor() as cur:
            cur.execute("""
SELECT
   ip_start,ip_count,country_code
FROM
   mainte2.RIR_ipv4_allocated
WHERE
   ip_start LIKE %(partial_match)s
ORDER BY
 LPAD(SPLIT_PART(ip_start,'.',1), 3, '0') || '.' ||
 LPAD(SPLIT_PART(ip_start,'.',2), 3, '0') || '.' ||
 LPAD(SPLIT_PART(ip_start,'.',3), 3, '0') || '.' ||
 LPAD(SPLIT_PART(ip_start,'.',4), 3, '0')""",
                        ({'partial_match': like_ip}))
            # レコード取得件数チェック
            if cur.rowcount > 0:
                rows: List[Tuple[str, int, str]] = cur.fetchall()
                if logger is not None:
                    logger.debug(f"rows.size: {len(rows)}")
                    for row in rows:
                        logger.debug(f"{row}")
                result = rows
            else:
                # マッチしなかったら空のリスト
                result = []
        return result
    except (Exception, psycopg2.DatabaseError) as err:
        raise err


def get_matches_main(
        conn: connection,
        target_ip: str,
        logger: Optional[logging.Logger] = None) -> Optional[List[Tuple[str, int, str]]]:
    def make_like_ip(like_old: str) -> Optional[str]:
        # 末尾の likeプレースホルダを削除する
        raw_ip: str = like_old.replace(".%", "")
        fields: List[str] = raw_ip.split(".")
        # コンマで区切って残りが1つなら終了
        field_size: int = len(fields)
        if field_size == 1:
            return None

        # フィールドを1つ減らす
        del fields[field_size - 1]
        # 末尾にlikeプレースホルダ(".%")を付加して終了
        return ".".join(fields) + ".%"

    target_ip_addr: IPv4Address = ip_address(target_ip)  # type: ignore
    like_ip: Optional[str] = make_like_ip(target_ip)
    matches: Optional[List[Tuple[str, int, str]]] = None
    while like_ip is not None:
        matches = get_rir_table_matches(conn, like_ip, logger=logger)
        if len(matches) > 0:
            # 先頭レコードの開始IPアドレス
            first_ip: str = matches[0][0]
            first_ip_addr: IPv4Address = ip_address(first_ip)  # type: ignore
            # 最終レコードの開始IPアドレス
            last: Tuple[str, int, str] = matches[-1]
            last_ip: str = last[0]
            ip_cnt: int = int(last[1])
            last_ip_addr: IPv4Address = ip_address(last_ip)  # type: ignore
            # 最終レコードのブロードキャストアドレス計算
            broadcast_addr: IPv4Address = last_ip_addr + ip_cnt - 1  # type: ignore
            if logger is not None:
                logger.info(f"match_first: {first_ip}, match_last: {last_ip}")

            if first_ip_addr < target_ip_addr < broadcast_addr:
                # ターゲットIPが先頭レコードの開始IPと最終レコードのブロードキャストの範囲内なら終了
                if logger is not None:
                    logger.debug(
                        f"Range in ({first_ip} < {target_ip} < {str(broadcast_addr)})"
                        f", break"
                    )
                break
            else:
                # 範囲外: 次のlike検索文字列を生成して検索処理に戻る
                like_ip = make_like_ip(like_ip)
                if logger is not None:
                    logger.info(f"next {like_ip} continue.")
        else:
            # レコード無し: 次のlike検索文字列を生成して検索処理に戻る
            if logger is not None:
                logger.info(f"{like_ip} is no match.")
            like_ip = make_like_ip(like_ip)
    return matches


def detect_cc_in_matches(
        target_ip: str,
        matches: List[Tuple[str, int, str]],
        logger: Optional[logging.Logger] = None) -> Tuple[Optional[str], Optional[str]]:
    def next_record(rows: List[Tuple[str, int, str]]) -> Iterator[RirRecord]:
        for (ip_sta, ip_cnt, cc) in rows:
            yield RirRecord(ip_start=ip_sta, ip_count=ip_cnt, country_code=cc)

    target_ip_addr: IPv4Address = ip_address(target_ip)  # type: ignore
    match_network: Optional[str] = None
    match_cc: Optional[str] = None
    rec: RirRecord
    for rec in next_record(matches):
        # ターゲットIP が ネットワークIPアドレスより大きい場合は範囲外のため処理終了
        if ip_address(rec.ip_start) > target_ip_addr:  # type: ignore
            if logger is not None:
                logger.debug(
                    f"{target_ip} < {rec.ip_start} break. No more match."
                )
            # マッチするデータなし
            break

        # 開始ネットワークIPのブロードキャストアドレスがターゲットIPより小さければ次のレコードへ
        broadcast_addr: IPv4Address = (
                ip_address(rec.ip_start) + rec.ip_count - 1)  # type: ignore
        if broadcast_addr < target_ip_addr:
            if logger is not None:
                logger.debug(f"({str(broadcast_addr)} < {target_ip}) -> continue")
            continue

        cidr_cc_list: List[Tuple[IPv4Network, str]] = get_cidr_cc_list(
            **asdict(rec)
        )
        if logger is not None:
            logger.debug(cidr_cc_list)
        match_network, match_cc = detect_cc_in_cidr_cc_list(target_ip, cidr_cc_list)
        break

    return match_network, match_cc


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


def read_cc_name_csv_todict(f_path: str) -> Dict[str, str]:
    result: Dict[str, str] = dict()
    lines: List[str] = read_csv(f_path)
    # ヘッダー: "country_code","japanese_name","public_name"
    for line in lines:
        fileds: List[str] = line.split(",")
        # 国コード: 日本語表記
        result[fileds[0]] = fileds[1]
    return result


def exec_main():
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-ip", required=True, type=str,
                        help="IP address.")
    parser.add_argument("--enable-debug", action="store_true",
                        help="Enable logger debug out.")
    # 国名コードと国名称CSVファイル ※任意
    parser.add_argument("--cc-name-csv", type=str,
                        help="Country code name CSV file.")
    args: argparse.Namespace = parser.parse_args()
    target_ip: str = args.target_ip
    enable_debug: bool = args.enable_debug

    # 国名コードと国名CSVファイルが指定されている場合はファイル存在チェック
    dict_cc_name: Optional[Dict[str, str]] = None
    cc_name_csv_file: Optional[str] = args.cc_name_csv
    if cc_name_csv_file is not None:
        if not os.path.exists(cc_name_csv_file):
            app_logger.warning(f"{cc_name_csv_file} not found!")
            exit(1)

        # CSVファイル読み込み
        dict_cc_name = read_cc_name_csv_todict(cc_name_csv_file)

    db: Optional[pgdatabase.PgDatabase] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, logger=None)
        conn: connection = db.get_connection()
        matches: Optional[List[Tuple[str, int, str]]] = get_matches_main(
            conn, target_ip, logger=app_logger if enable_debug else None)
    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        exit(1)
    except Exception as exp:
        app_logger.error(exp)
        exit(1)
    finally:
        if db is not None:
            db.close()

    # ターゲットIPのネットワーク(CIDR表記)と国コードを取得する
    if matches is not None and len(matches) > 0:
        network: Optional[str]
        cc: Optional[str]
        network, cc = detect_cc_in_matches(
            target_ip, matches, logger=app_logger if enable_debug else None)
        if network is not None and cc is not None:
            # 国名コードと国名(日本語表記) 辞書オブジェクトが存在したら国名(日本語表記)を取得
            jp_name: Optional[str] = None
            if dict_cc_name is not None:
                jp_name = dict_cc_name.get(cc)
            if jp_name is not None:
                # 国コードと国名(日本語表記)を出力
                app_logger.info(
                    f'Find {target_ip} in (network: "{network}", "{cc}:{jp_name}")'
                )
            else:
                app_logger.info(
                    f'Find {target_ip} in (network: "{network}", country_code: "{cc}")'
                )
        else:
            app_logger.info(f"Not match in data.")
    else:
        # このケースは想定しない
        app_logger.warning(f"Not exists in RIR table.")


if __name__ == '__main__':
    exec_main()
