import argparse
import logging
import os
import pprint

from typing import Dict, Optional, Tuple
from http.client import HTTPResponse, IncompleteRead
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, ParseResult
from urllib.request import Request, urlopen

SAVE_DIR: str = os.path.expanduser("~/Videos/script")

# ユーザーエージェント
UA: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 "
           "Firefox/107.0")

# ダウンロード用のリクエストヘッダー
REQ_HEADERS: Dict[str, str] = {
    "Accept": "*/*",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive"
}

app_logger: logging.Logger = logging.getLogger(__name__)
handler: logging.Handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
app_logger.addHandler(handler)


def basename_in_url(url: str, is_image: Optional[bool] = None) -> str:
    parsed: ParseResult = urlparse(url)
    # check file extention
    lastname: str = os.path.basename(parsed.path)
    if is_image:
        return lastname

    dot_pos: int = lastname.find(".")
    return lastname[:dot_pos] if dot_pos != -1 else lastname


def download(url: str,
             save_path: str,
             headers: Dict[str, str]) -> Tuple[str, int]:
    app_logger.debug(f"Download url: {url}")

    req: Request = Request(url, headers=headers)
    app_logger.debug("** Request headers **")
    app_logger.debug(pprint.pformat(req.headers, indent=2))

    resp: HTTPResponse = urlopen(req, timeout=5.)

    app_logger.info(f"response.code: {resp.status}")
    # python 3.9 で非推奨
    # if resp.getcode() != 200:
    if resp.status != 200:
        # 200以外はエラーとする
        raise HTTPError(
            url, resp.status, "Disable download!",
            resp.info(), None
        )

    app_logger.debug("** Response headers **")
    app_logger.debug(resp.info())
    # Content-Length ヘッダーチェック
    raw_content_len: str = resp.info()["Content-Length"]
    app_logger.debug(f"Content-Length: {raw_content_len}")
    if raw_content_len is None:
        # ダウンロードできない: 411 Length Required
        raise HTTPError(
            url, 411, "Server did not send Content-Length!",
            resp.info(), None
        )

    content_length: int = int(raw_content_len.strip())
    app_logger.info(f"Content-Length: {content_length:,}")

    # ファイル保存処理
    dl_size: int = 0
    show_cnt: int = 0
    with open(save_path, 'wb') as fp:
        while True:
            try:
                # Read buffer: 8KB
                buff: bytes = resp.read(1024 * 8)
            except IncompleteRead as e:
                app_logger.warning(f"Downloaded: {dl_size}/{content_length}")
                app_logger.error("Read Error: %r", e)
                raise e

            if not buff:
                break

            dl_size += len(buff)
            show_cnt += len(buff)

            # @10KB (output) downloading: 12,345,678
            if show_cnt > (1024 * 1024):
                app_logger.debug(f"downloading: {dl_size:,}")
                show_cnt = 0

            fp.write(buff)
            if dl_size >= content_length:
                app_logger.debug(f"Done downloaded: {dl_size:,}")
                break

    return save_path, content_length


def main():
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # 動画URL
    parser.add_argument("--url", type=str, required=True,
                        help="Download Video URL.")
    # リファラーURL ※任意
    parser.add_argument("--referer-url", type=str,
                        help="Referer URL with video URL, optional.")
    # DEBUG出力するか: 指定があれば出力する
    parser.add_argument("--is-debug", action='store_true',
                        help="Output DEBUG.")
    args: argparse.Namespace = parser.parse_args()
    is_debug: bool = args.is_debug
    if is_debug:
        app_logger.setLevel(logging.DEBUG)
    else:
        app_logger.setLevel(logging.INFO)

    video_url: str = args.url
    # 保存ファイル名: 動画URLのパスの末尾名
    save_name: str = basename_in_url(video_url, is_image=True)
    save_path: str = os.path.join(SAVE_DIR, save_name)

    # リクエストヘッダーの設定
    # User-Agent
    REQ_HEADERS["User-Agent"] = UA
    # リファラーURLが指定されていたらリファラーヘッダーを追加
    if args.referer_url is not None:
        REQ_HEADERS["Referer"] = args.referer_url

    try:
        saved_path, file_size = download(
            video_url, save_path=save_path, headers=REQ_HEADERS
        )
        app_logger.info(f"Saved: {saved_path}")
        app_logger.info(f"FileSize: {file_size:,}")
        app_logger.info("Download finished.")
    except HTTPError as err:
        # エラー時のレスポンスコート
        app_logger.warning(f"{err}\n >> {video_url}")
        app_logger.warning("** Response headers **")
        app_logger.warning(f"{err.headers.as_string()}")
    except URLError as err:
        app_logger.warning(f"{err.reason}\n >> {video_url}")
    except Exception as e:
        app_logger.error(f"{e}\n >>  {video_url}")


if __name__ == '__main__':
    main()
