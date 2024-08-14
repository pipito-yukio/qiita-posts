import argparse
import csv
import logging
import os
from collections import OrderedDict
from datetime import date
from dataclasses import asdict, dataclass
from ipaddress import ip_address, summarize_address_range, IPv4Address, IPv4Network
import typing
from typing import List, Dict, Iterator, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection, cursor

from db import pgdatabase

"""
[Qiita投稿No38用スクリプト]
不正アクセスIPカウンターCSVファイルのIPアドレスの国コードを一括取得しファイル保存する
"""

# データベース接続情報
DB_CONF_FILE: str = os.path.join("conf", "db_conn.json")
# 国コード不明
CC_UNKNOWN: str = "??"


# ネットワークアドレス(Ipv4)の国コードとホストIPアドレスリストを保持するデータクラス
@dataclass(frozen=True)
class IpNetworkWithCC:
    ip_network: str
    country_code: str
    ip_hosts: list[str]


# RIR データクラス
@dataclass(frozen=True)
class RirRecord:
    ip_start: str
    ip_count: int
    country_code: str


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


def write_text_lines(file_name: str, save_list: List[str],
                     append_file: bool = False) -> None:
    save_mode: str = 'a' if append_file else 'w'
    with open(file_name, save_mode) as fp:
        for save_line in save_list:
            fp.write(f"{save_line}\n")
        fp.flush()


@typing.no_type_check
def get_cidr_cc_list(ip_start: str,
                     ip_count: int,
                     country_code: str) -> List[Tuple[IPv4Network, str]]:
    addr_first: IPv4Address = ip_address(ip_start)
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
        # ゼロ埋めしたIPアドレスの昇順にソートする
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


@typing.no_type_check
def match_greater_target(
        target_ip_addr: IPv4Address, ip_start: str) -> bool:
    ip_start_addr: IPv4Address = ip_address(ip_start)
    return ip_start_addr > target_ip_addr


def detect_cc_in_matches(
        target_ip_addr: IPv4Address,
        matches: List[Tuple[str, int, str]],
        param_dict_ip_net: Optional[Dict[str, IpNetworkWithCC]] = None,
        logger: Optional[logging.Logger] = None) -> Tuple[Optional[str], Optional[str]]:
    def next_record(rows: List[Tuple[str, int, str]]) -> Iterator[RirRecord]:
        for (ip_sta, ip_cnt, country_code) in rows:
            yield RirRecord(ip_start=ip_sta, ip_count=ip_cnt, country_code=country_code)

    match_network: Optional[str] = None
    match_cc: Optional[str] = None
    rec: RirRecord
    for rec in next_record(matches):
        # ターゲットIP が ネットワークIPアドレスより大きい場合は処理終了
        if match_greater_target(target_ip_addr, rec.ip_start):
            break

        # 開始ネットワークIPのブロードキャストアドレスがターゲットIPより小さければ次のレコードへ
        broadcast_addr: IPv4Address = (
                ip_address(rec.ip_start) + rec.ip_count - 1)  # type: ignore
        if broadcast_addr < target_ip_addr:
            continue

        cidr_cc_list: List[Tuple[IPv4Network, str]] = get_cidr_cc_list(**asdict(rec))
        if logger is not None:
            logger.debug(cidr_cc_list)
        match_network, match_cc = detect_cc_in_cidr_cc_list(
            str(target_ip_addr), cidr_cc_list
        )
        if match_network is not None and match_cc is not None:
            # ネットワークと国コードが取得できた
            if param_dict_ip_net is not None:
                # IPネットワークに属する全てのホストをリストに追加
                data: Optional[IpNetworkWithCC] = param_dict_ip_net.get(match_network)
                if data is None:
                    # 辞書オブジェクトに存在しない場合はレコードを追加
                    param_dict_ip_net[match_network] = IpNetworkWithCC(
                        ip_network=match_network,
                        country_code=match_cc,
                        ip_hosts=[str(target_ip_addr)]
                    )
                else:
                    # 辞書オブジェクトに存在したらターケットをホストリストに追加
                    data.ip_hosts.append(str(target_ip_addr))

    return match_network, match_cc


def sorted_ip_addr_list(csv_lines: List[str]) -> List[str]:
    # CSVからIPアドレス(2列目)のみを取得
    ip_list: List[str] = [csv_line.split(",")[1] for csv_line in csv_lines]
    # IPアドレスをドットで分割した各パートごとに前ゼロ3桁に加工する
    ip_full_list: List = []
    for ip_item in ip_list:
        ip_4 = ip_item.split('.')
        full = f"{int(ip_4[0]):03}.{int(ip_4[1]):03}.{int(ip_4[2]):03}.{int(ip_4[3]):03}"
        ip_full_list.append(full)
    # 前ゼロ3桁に加工したIPアドレスを昇順でソートする
    sorted_ip_list: List = sorted(ip_full_list)
    # ソート済みIPアドレスを元に戻す
    sorted_org_ip_list: List = []
    for full_ip in sorted_ip_list:
        fields = full_ip.split('.')
        org_ip = f"{int(fields[0])}.{int(fields[1])}.{int(fields[2])}.{int(fields[3])}"
        sorted_org_ip_list.append(org_ip)
    return sorted_org_ip_list


def rir_table_matches_main(
        conn: connection,
        target_ip_list: List[str],
        dict_ip_network_cc: Optional[Dict[str, IpNetworkWithCC]],
        unknown_ip_list: Optional[List[str]],
        logger: logging.Logger,
        enable_debug: bool = False) -> None:
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

    for i, target_ip in enumerate(target_ip_list):
        logger.info(f"{i + 1:04d}: START {target_ip}")
        target_ip_addr: IPv4Address = ip_address(target_ip)  # type: ignore
        like_ip: Optional[str] = make_like_ip(target_ip)
        matches: Optional[List[Tuple[str, int, str]]] = None
        while like_ip is not None:
            matches = get_rir_table_matches(
                conn, like_ip, logger=logger if enable_debug else None
            )
            if len(matches) > 0:
                match_first: Tuple[str, int, str] = matches[0]
                if enable_debug:
                    logger.debug(f"{i + 1:04d}: match_first_ip: {match_first[0]}")
                match_greader: bool = match_greater_target(target_ip_addr,
                                                           match_first[0])
                if match_greader:
                    # 範囲外のネットワークIP
                    if enable_debug:
                        logger.debug(f"({match_first[0]} > {target_ip}) continue")
                    # 次のlike検索を実行
                    like_ip = make_like_ip(like_ip)
                else:
                    # 先頭のレコードが範囲内のネットワークIPアドレスなら終了
                    if enable_debug:
                        logger.debug(f"({match_first[0]} <= {target_ip}) break")
                    break
            else:
                # レコード無し: 次のlike検索文字列を生成して検索処理に戻る
                if enable_debug:
                    logger.debug(f"{like_ip} is no match.")
                like_ip = make_like_ip(like_ip)

        # ターケットIPが属するネットワークアドレスと国コードを取得する
        upd_cc: Optional[str]
        if matches is not None and len(matches) > 0:
            match_network: Optional[str]
            match_cc: Optional[str]
            match_network, match_cc = detect_cc_in_matches(
                target_ip_addr,
                matches,
                param_dict_ip_net=dict_ip_network_cc,
                logger=logger if enable_debug else None
            )
            upd_cc = match_cc if match_cc is not None else CC_UNKNOWN
            logger.info(f"{i + 1:04d}: END   {target_ip}, {upd_cc}")
            if upd_cc == CC_UNKNOWN:
                if unknown_ip_list is not None:
                    unknown_ip_list.append(target_ip)
        else:
            # 一致なし
            logger.warning(
                f"{i + 1:04d}: END   {target_ip}, RIR_ipv4_allocated no match."
            )
            if unknown_ip_list is not None:
                unknown_ip_list.append(target_ip)


def save_network_cc_dict(
        date_part: str, save_dir: str, dict_ip_network_cc: Dict[str, IpNetworkWithCC],
        logger: logging.Logger) -> None:
    file_name: str = f"ip_network_cc_with_hosts_{date_part}.txt"
    save_file: str = os.path.join(save_dir, file_name)
    # 行形式: "ip_network","cc",["host1","host2",...]
    net_cc_rec: IpNetworkWithCC
    net_cc_lines: List[str] = []
    for key in dict_ip_network_cc.keys():
        net_cc_rec = dict_ip_network_cc[key]
        line_hosts: str = '","'.join(net_cc_rec.ip_hosts)
        line: str = f'"{key}","{net_cc_rec.country_code}",["{line_hosts}"]'
        net_cc_lines.append(line)
    write_text_lines(save_file, net_cc_lines)
    logger.info(f"Saved: {save_file}")


def save_unknown_list(
        date_part: str, save_dir: str, unknown_ip_list: List[str],
        logger: logging.Logger) -> None:
    file_name: str = f"unknown_ip_hosts_{date_part}.txt"
    save_file: str = os.path.join(save_dir, file_name)
    write_text_lines(save_file, unknown_ip_list)
    logger.info(f"Saved: {save_file}")


def export_main():
    logging.basicConfig(format='%(levelname)s %(message)s')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-file", type=str, required=True,
                        help="CSV file path.")
    parser.add_argument("--enable-debug", action="store_true",
                        help="Enable logger debug out.")
    args: argparse.Namespace = parser.parse_args()

    # CSVファイルのパスチェック
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

    enable_debug: bool = args.enable_debug
    # ソート済みのチェック用IPアドレスリスト
    target_ip_list: List[str] = sorted_ip_addr_list(csv_lines)
    # ファイルの出力先ディレクトリ
    output_dir: str = os.path.expanduser("~/Documents/qiita")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # ターゲットIPが属するネットワーク情報を保持する辞書オブジェクト
    dict_ip_network_cc: Dict[str, IpNetworkWithCC] = OrderedDict()
    # 国コードが不明のIPアドレスリスト
    unknown_ip_list: List[str] = []

    db: Optional[pgdatabase.PgDatabase] = None
    try:
        db = pgdatabase.PgDatabase(DB_CONF_FILE, logger=None)
        conn: connection = db.get_connection()
        rir_table_matches_main(
            conn, target_ip_list, dict_ip_network_cc, unknown_ip_list,
            app_logger, enable_debug
        )

    except psycopg2.Error as db_err:
        app_logger.error(db_err)
        exit(1)
    except Exception as err:
        app_logger.error(err)
        exit(1)
    finally:
        if db is not None:
            db.close()

    # ターゲットIPの属するIPネットワークと国コード情報ファイル
    date_part: str = date.today().isoformat()
    if len(dict_ip_network_cc):
        save_network_cc_dict(date_part, output_dir, dict_ip_network_cc, app_logger)

    # 国コード不明IPアドレスリストの保存
    if len(unknown_ip_list) > 0:
        save_unknown_list(date_part, output_dir, unknown_ip_list, app_logger)

    app_logger.info("Done.")


if __name__ == '__main__':
    export_main()
