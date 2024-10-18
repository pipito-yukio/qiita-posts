import argparse
import csv
import logging
import os
import typing
from typing import Iterator, List, Optional, Tuple

from ipaddress import (
    ip_address, summarize_address_range, IPv4Address, IPv4Network
)


"""
RIR ipv4 allocated CSV file to cidr network.
"""

OUT_CSV_HEADER: str = '"network_addr","country_code","allocated_date","registry_id"\n'


def paginate(reader: Iterator, page_size):
    pages: List = []
    it: Iterator = iter(reader)
    while True:
        try:
            for i in range(page_size):
                pages.append(next(it))
            yield pages
            pages = []
        except StopIteration:
            if pages:
                yield pages
            return


def get_file_reader(file_name: str):
    fp = open(file_name, mode='r')
    return fp


def get_file_writer(file_name: str):
    fp = open(file_name, mode='w')
    return fp


def ip_start_to_cidr_network(csv_lines: List[List]) -> List[str]:
    @typing.no_type_check
    def get_cidr_list(ip_start: str,
                      ip_count: int) -> List[str]:
        addr_first: IPv4Address = ip_address(ip_start)
        addr_last: IPv4Address = addr_first + ip_count - 1
        cidr_ite: Iterator[IPv4Network] = summarize_address_range(addr_first, addr_last)
        return [str(network) for network in cidr_ite]

    result: List[str] = []
    for csv_line in csv_lines:
        cc: str = csv_line[2]
        alloc_date: str = csv_line[3]
        registry_id: int = int(csv_line[4])
        cidr_list: List[str] = get_cidr_list(csv_line[0], int(csv_line[1]))
        for cidr in cidr_list:
            out_line: str = f'"{cidr}","{cc}","{alloc_date}",{registry_id}\n'
            result.append(out_line)
    return result


def batch_main():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-file", type=str, required=True,
                        help="Source csv file path.")
    # 変換後のCSVファイル保存先 ※未指定なら --csv-file のディレクトリ
    parser.add_argument("--output-dir", type=str,
                        help="Output csv file directory.")
    parser.add_argument("--page-size", type=int,
                        default=5000,
                        help="CSV row count size.")
    args: argparse.Namespace = parser.parse_args()
    csv_file: str = args.csv_file
    output_dir: Optional[str] = args.output_dir
    app_logger.info(f"csv_file: {csv_file}")
    # 元のCSVファイルチェック
    f_path: str
    if csv_file.find("~/") == 0:
        f_path = os.path.expanduser(csv_file)
    else:
        f_path = csv_file
    if not os.path.exists(f_path):
        app_logger.error(f"FileNotFound: {f_path}")
        exit(1)

    # 出力ファイル名: オリジナル名にアンダースコア修飾する(xxx_cidr.csv)
    file_name: str = os.path.basename(f_path)
    names: Tuple[str, str] = os.path.splitext(file_name)
    out_name: str = f"{names[0]}_cidr{names[1]}"
    # 出力先
    output_file: str
    if output_dir is None:
        # 出力先が未指定ならソースCSVのディレクトリ
        dir_name: str = os.path.dirname(f_path)
        output_file = os.path.join(dir_name, out_name)
    else:
        # 指定ディレクトリ
        output_file = os.path.join(os.path.expanduser(output_dir), out_name)

    # ページサイズ
    page_size: int = args.page_size

    # ファイル操作オブジェクトを開く
    fp_reader = get_file_reader(f_path)
    app_logger.debug(fp_reader)
    fp_writer = get_file_writer(output_file)
    app_logger.debug(fp_writer)
    try:
        csv_reader = csv.reader(fp_reader, delimiter=',', dialect='unix')
        # ソースCSVのヘッダースキップ
        next(csv_reader)
        # 出力CSVのヘッダー出力
        fp_writer.write(OUT_CSV_HEADER)
        fp_writer.flush()

        # データ読み込み
        input_total: int = 0
        output_total: int = 0
        csv_lines: List[List]
        # RIRデータのCSVは約20万レコード以上あるので指定さりたページサイズ(行数)でファイルに保存
        for csv_lines in paginate(csv_reader, page_size=page_size):
            input_total += len(csv_lines)
            out_lines: List[str] = ip_start_to_cidr_network(csv_lines)
            output_total += len(out_lines)
            app_logger.info(
                f"{input_total}, input_lines: {input_total}, output_lines:{output_total}"
            )
            fp_writer.writelines(out_lines)
            fp_writer.flush()

    except Exception as ex:
        app_logger.error(ex)
        exit(1)
    finally:
        if fp_reader is not None:
            fp_reader.close()
        if fp_writer is not None:
            fp_writer.close()

    app_logger.info(f"Saved {output_file}")


if __name__ == '__main__':
    batch_main()
