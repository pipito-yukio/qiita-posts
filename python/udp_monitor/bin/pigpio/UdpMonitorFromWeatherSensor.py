import io
import logging
import os
import signal
import socket
from datetime import datetime
from typing import List, Optional, Tuple
from log import logsetting

"""
UDP packet Monitor from ESP Weather sensors With export CSV
[UDP port] 2222

For ubuntu permit 2222/udp
$ sudo firewall-cmd --add-port=2222/udp --permanent
success
"""

# args option default
# システムサービスで設定される環境変数は文字列なので、デフォルトが数値であっても文字列にする
WEATHER_UDP_PORT: str = os.environ.get("WEATHER_UDP_PORT", "2222")
CSV_OUTPUT_PATH: str =  os.environ.get("CSV_OUTPUT_PATH", "~/Documents/csv")
CSV_FILE: str = "udp_weather.csv"
CSV_HEADER: str = '"measurement_time","device_name","temp_out","temp_in","humid","pressure"\r\n'
BUFF_SIZE: int = 1024
isLogLevelDebug: bool = False


def detect_signal(signum, frame):
    """
    Detect shutdown, and execute cleanup.
    :param signum: Signal number
    :param frame: frame
    :return:
    """
    logger.info("signum: {}, frame: {}".format(signum, frame))
    if signum == signal.SIGTERM or signum == signal.SIGSTOP:
        # signal shutdown
        cleanup()
        # Current process terminate
        exit(0)


def cleanup():
    global csv_fp
    if csv_fp is not None:
        csv_fp.close()
    udp_client.close()


def loop(client: socket.socket, fp: io.TextIOWrapper):
    server_ip = ''
    data: bytes
    addr: str
    while True:
        data, addr = client.recvfrom(BUFF_SIZE)
        if server_ip != addr:
            server_ip = addr
            logger.info(f"server ip: {server_ip}")

        # from ESP output: device_name, temp_out, temp_in, humid, pressure
        line: str = data.decode("utf-8")
        record: List = line.split(",")
        # Insert weather DB with local time
        if isLogLevelDebug:
            logger.debug(line)
        # 到着時刻
        now_timestamp: datetime = datetime.now()
        s_timestamp: str = now_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        line: str = f'"{s_timestamp}","{record[0]}",{record[1]},{record[2]},{record[3]},{record[4]}\r\n'
        fp.write(line)
        fp.flush()

if __name__ == '__main__':
    logger: logging.Logger = logsetting.create_logger("service_weather")
    isLogLevelDebug = logger.getEffectiveLevel() <= logging.DEBUG
    # サービス停止 | シャットダウンフック
    signal.signal(signal.SIGTERM, detect_signal)

    hostname: str = socket.gethostname()
    # Receive broadcast.
    # ポート番号は文字列なので整数に変換する
    broad_address: Tuple[str, int] = ("", int(WEATHER_UDP_PORT))
    logger.info(f"{hostname}: {broad_address}")
    # UDP client
    udp_client: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_client.bind(broad_address)

    # 出力先ディレクトリが存在しなければ作成する
    output_full_path: str = os.path.expanduser(CSV_OUTPUT_PATH)
    if not os.path.exists(output_full_path):
        os.makedirs(output_full_path)
    output_filepath: str = os.path.join(output_full_path, CSV_FILE)
    # CSVファイル TextIOオブジェクト定義
    csv_fp: Optional[io.TextIOWrapper] = None
    try:
        if os.path.exists(output_filepath):
            # 新規なら追記モード
            csv_fp = open(output_filepath, 'a', encoding="utf-8")
        else:
            # 既存ファイルなら新規゜書き込みモード
            csv_fp = open(output_filepath, 'w', encoding="utf-8")
            # ヘッダー追記
            csv_fp.write(CSV_HEADER)
            csv_fp.flush()

        # UDPモニターループ
        logger.info(f"type(csv_fp): {type(csv_fp)}")
        loop(udp_client, csv_fp)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupted!")
    finally:
        cleanup()
