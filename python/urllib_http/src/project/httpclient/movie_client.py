import json
import logging
import os
import pprint

from http.client import HTTPResponse, IncompleteRead
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urlparse, ParseResult
from typing import Dict, Optional, Tuple

# 接続タイムアウト
CONN_TIMEOUT: float = 5.
# 設定ファイルで上書き
# conf/download_spec.json
# クラスレベルで上書き可能な設定値
buff_size: int = 1024 * 8
# @ 1MB
debug_print_break_size: int = 1024 * 1024

# リクエストヘッダー
# conf/http_client.json
req_headers: Dict[str, str] = {}


def basename_in_url(url: str, is_image: Optional[bool] = None) -> str:
    parsed: ParseResult = urlparse(url)
    # check file extension
    lastname: str = os.path.basename(parsed.path)
    if is_image:
        return lastname

    dot_pos: int = lastname.find(".")
    return lastname[:dot_pos] if dot_pos != -1 else lastname


class MovieDownloadClient(object):
    @classmethod
    def init(cls, conf_dir: str):
        global buff_size, debug_print_break_size
        global req_headers
        # ダウンローダ用設定値
        with open(os.path.join(conf_dir, "download_spec.json")) as fp:
            conf = json.load(fp)
        buff_size = eval(conf["bufferSize"])
        debug_print_break_size = eval(conf["debugPrintBreakSize"])
        # リクエストヘッダーなどの設定値
        with open(os.path.join(conf_dir, "http_client.json")) as fp:
            conf = json.load(fp)
        req_headers = conf["downloadHeaders"]
        req_headers["User-Agent"] = conf["userAgent"]

    def __init__(self, save_dir: str, logger: logging.Logger):
        self.save_dir = save_dir
        self.logger: logging.Logger = logger
        # モジュール変数のリクエストヘッダーをオブジェクトのヘッダーに設定する
        self.headers: Dict[str, str] = req_headers

    def download(self,
                 url: str,
                 referer_url: Optional[str] = None) -> Tuple[str, int]:
        self.logger.debug(f"Download url: {url}")
        if referer_url is not None:
            self.logger.debug(f"Referer url: {referer_url}")

        # リファラーの有無
        if referer_url is not None:
            self.headers['Referer'] = referer_url

        req: Request = Request(url, headers=self.headers)
        self.logger.debug("** Request headers **")
        self.logger.debug(pprint.pformat(req.headers, indent=2))

        resp: HTTPResponse = urlopen(req, timeout=CONN_TIMEOUT)
        self.logger.info(f"response.code: {resp.status}\n >> {url}")
        if resp.status != 200:
            # 200以外はエラーとする
            raise HTTPError(
                url, resp.status, "Disable download!",
                resp.info(), None
            )

        self.logger.debug("** Response headers **")
        self.logger.debug(resp.info())
        # Content-Length ヘッダーチェック
        raw_content_len: str = resp.info()["Content-Length"]
        self.logger.debug(f"Content-Length: {raw_content_len}")
        if raw_content_len is None:
            # ダウンロードできない: 411 Length Required
            raise HTTPError(
                url, 411, "Server did not send Content-Length!",
                resp.info(), None
            )

        content_length: int = int(raw_content_len.strip())
        self.logger.info(f"Content-Length: {content_length:,}")

        # ファイル保存処理
        file_name: str = basename_in_url(url, is_image=True)
        save_path: str = os.path.join(self.save_dir, file_name)
        dl_size: int = 0
        show_cnt: int = 0
        with open(save_path, 'wb') as fp:
            while True:
                try:
                    buff: bytes = resp.read(buff_size)
                except IncompleteRead as e:
                    self.logger.warning(f"Downloaded: {dl_size}/{content_length}")
                    self.logger.error("Read Error: %r", e)
                    raise e

                if not buff:
                    break

                dl_size += len(buff)
                show_cnt += len(buff)
                if show_cnt > debug_print_break_size:
                    self.logger.debug(f"downloading: {dl_size:,}")
                    show_cnt = 0

                fp.write(buff)
                if dl_size >= content_length:
                    self.logger.debug(f"Done downloaded: {dl_size:,}")
                    break

        return save_path, content_length
