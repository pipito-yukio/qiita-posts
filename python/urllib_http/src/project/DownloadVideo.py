import argparse
import logging
import os

from urllib.error import HTTPError, URLError

from httpclient import movie_client

SAVE_DIR: str = os.path.expanduser("~/Videos/script")

app_logger: logging.Logger = logging.getLogger(__name__)
handler: logging.Handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
app_logger.addHandler(handler)


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

    # ダウンローダー初期化
    #  (1) 読み込みバッファサイズを設定ファイルから読み込み
    #  (2) リクエストヘッダーを設定ファイルから読み込み
    movie_client.MovieDownloadClient.init(conf_dir="conf")
    # ダウンローダーオブジェクト生成
    client = movie_client.MovieDownloadClient(SAVE_DIR, app_logger)
    video_url: str = args.url
    try:
        saved_path, file_size = client.download(video_url, referer_url=args.referer_url)
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
