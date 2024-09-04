import argparse
import logging
import os
import re
from collections import Counter
from datetime import date
from typing import List, Optional, Set, Tuple

"""
Qiita投稿用スクリプト
自宅サーバー取得したSSH不正ログインエラーログからIPアドレスの出現数を集計するスクリプト
"""

# rhhost の後ろに何もないケースと "user=xxx"があるパターンがある
# authentication failure; logname= uid=0 euid=0 tty=ssh ruser= rhost=216.181.226.86
# authentication failure; logname= uid=0 euid=0 tty=ssh ruser= rhost=218.92.0.96  user=root
re_auth_fail: re.Pattern = re.compile(
    r"^.+?rhost=([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}).*$"
)
# ファイル名のログ日付抽出
re_log_file: re.Pattern = re.compile(r"^AuthFail_ssh_(\d{4}-\d{2}-\d{2})\.log$")


def read_text(file_name: str) -> List[str]:
    with open(file_name, 'r') as fp:
        lines: List[str] = [ln for ln in fp]
        return lines


def write_csv(
        file_name: str, save_list: List[str],
        header: Optional[str] = None) -> None:
    with open(file_name, 'w') as fp:
        if header is not None:
            fp.write(f"{header}\n")
        for line in save_list:
            fp.write(f"{line}\n")
        fp.flush()


def save_csvfile(param_save_path: str, filename: str, csv_list: List[str]) -> str:
    save_path: str
    if param_save_path.find("~") == 0:
        save_path = os.path.join(os.path.expanduser(param_save_path))
    else:
        save_path = param_save_path
    # 保存先ディレクトリ ※存在しなければデイレクトリを作成する
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    save_file: str = os.path.join(save_path, filename)
    write_csv(save_file, csv_list, header='"log_date","ip_addr","appear_count"')
    return save_file


def extract_log_date(file_path: str) -> str:
    # ファイル名から日付を取得
    b_name: str = os.path.basename(file_path)
    f_mat: Optional[re.Match] = re_log_file.search(b_name)
    if f_mat:
        return f_mat.group(1)
    else:
        return date.today().isoformat()


def extract_ip_tolist(lines: List[str]) -> List[str]:
    result: List = []
    for line in lines:
        mat: Optional[re.Match] = re_auth_fail.search(line)
        if mat:
            result.append(mat.group(1))
    return result


def convert_sorted_ip_list(ip_list: List[str]) -> List[str]:
    # カウンターオブジェクトから重複のないキーを取得
    ip_set: Set[str] = set(ip_list)
    # ソート用の前ゼロ加工したIPアドレスを格納するリスト
    full_ip_list: List[str] = []
    ip_4: List[str]
    for ip_addr in ip_set:
        ip_4 = ip_addr.split(".")
        # IPアドレスの各パーツを前ゼロ加工して結合する
        ip_full: str = (f"{int(ip_4[0]):03}.{int(ip_4[1]):03}."
                        f"{int(ip_4[2]):03}.{int(ip_4[3]):03}")
        full_ip_list.append(ip_full)
    # 前ゼロ加工したIPアドレスリストを数値的にソート
    sorted_list: List[str] = sorted(full_ip_list)
    # IPアドレスを元に戻す
    result: List[str] = []
    for full_ip in sorted_list:
        ip_4 = full_ip.split(".")
        # 各パーツに分解し、パーツごとに数値に戻して元のIPアドレスに戻す
        org_ip: str = f"{int(ip_4[0])}.{int(ip_4[1])}.{int(ip_4[2])}.{int(ip_4[3])}"
        result.append(org_ip)
    return result


def batch_main():
    logging.basicConfig(format="%(message)s")
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.INFO)

    # コマンドラインパラメータ
    # --log-file: エラーログファイル ※必須
    # (A) コンソール出力: --output console ※デフォルト
    #    --show-top: Top N, デフォルト(=0) 全て出力
    #    --sort-ip-addr: 指定された場合、前ゼロ加工したIPアドレスの昇順でソート
    # (B) CSVファイル出力: --output csv
    #    --save-path: CSVの出力先ディレクトリ ※デフォルト: "~/Documents/csv"
    #    --appear-threshold: 出現数の最小値 ※デフォルト: 30回
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", required=True, type=str,
                        help="Log File name.")
    parser.add_argument("--output", type=str,
                        choices=["console", "csv"], default="console",
                        help="Output console or csv file, default console.")
    parser.add_argument("--sort-ip-addr", action="store_true",
                        help="Sort ip-addr for output console.")
    parser.add_argument("--show-top", type=int, default=0,
                        help="IPアドレスのランキング(Top N 位), 規定値(=0)なら全て出力.")
    parser.add_argument("--appear-threshold", type=int, default=30,
                        help="出現数の閾値 N回以上。既定値=30回")
    # CSVファイルの保存先 ※未指定ならデフォルト "~/Documents/csv"
    parser.add_argument("--save-path", type=str, default="~/Documents/csv",
                        help="Save csv file path(Directory).")
    args: argparse.Namespace = parser.parse_args()
    log_file: str = args.log_file
    app_logger.info(f"log-file: {log_file}")

    # 出力モード
    output: str = args.output
    p_output: str = f"output: {output}"
    if output == "console":
        app_logger.info(
            f"{p_output}, show-top: {args.show_top}, sort-ip-addr: {args.sort_ip_addr}"
        )
    else:
        app_logger.info(f"{p_output}, appear-threshold: {args.appear_threshold}")
        app_logger.info(f"save_path: {args.save_path}")

    # エラーログファイルの存在チェック
    f_path: str
    if log_file.find("~/") == 0:
        f_path = os.path.expanduser(log_file)
    else:
        f_path = log_file
    if not os.path.exists(f_path):
        app_logger.error(f"FileNotFound: {f_path}")
        exit(1)

    # エラーログファイルの読み込み
    lines: List[str] = read_text(f_path)
    ip_list: List[str] = extract_ip_tolist(lines)
    list_size: int = len(ip_list)
    app_logger.info(f"ip_list.size: {list_size}")

    if list_size > 0:
        # 抽出した ip の出現数をカウント
        counter: Counter = Counter(ip_list)
        app_logger.info(f"counter.elements.size: {len(counter)}")
        if output == "csv":
            # ファイル出力する場合は出現回数の閾値
            appear_threshold: int = args.appear_threshold
            # ファイル名からログ日付を取り出す ※シェルスクリプトでログ日付がファイル名の末尾に付与されている
            log_date: str = extract_log_date(f_path)
            csv_list: List[str] = []
            # CSV出力: 出現回数が指定件数以上
            for (ip_addr, cnt) in counter.most_common():
                if cnt >= appear_threshold:
                    csv_line: str = f'"{log_date}","{ip_addr}",{cnt}'
                    csv_list.append(csv_line)
            app_logger.info(f"output_lines: {len(csv_list)}")
            # 保存ファイル名
            save_name: str = f"ssh_auth_error_{log_date}.csv"
            saved_file: str = save_csvfile(args.save_path, save_name, csv_list)
            app_logger.info(f"Saved: {saved_file}")
        else:
            # コンソール出力の場合: 全件 | Top N (1以上)
            most_common: List[Tuple[str, int]]
            if args.show_top == 0:
                # 全件
                most_common = counter.most_common()
            else:
                # Top N位
                most_common = counter.most_common(args.show_top)
            if args.sort_ip_addr:
                # 前ゼロ加工済みIPアドレスの昇順
                tmp_list: List[str] = [ip for (ip, cnt) in most_common]
                sorted_ip_list = convert_sorted_ip_list(tmp_list)
                for ip in sorted_ip_list:
                    app_logger.info(f"('{ip}', {counter[ip]})")
            else:
                # 出現回数のランキング(降順)
                for item in most_common:
                    app_logger.info(item)
    else:
        app_logger.info(f"不正アクセスに該当するIPアドレス未検出.")


if __name__ == '__main__':
    batch_main()

