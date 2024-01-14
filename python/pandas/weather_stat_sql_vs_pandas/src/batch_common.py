import sqlite3
from datetime import datetime, timedelta

"""
バッチアプリ共通定義
"""

# 出力画層用HTMLテンプレート (Bootstrap5.x with CDN)
OUT_HTML = """
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" 
 rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN"
 crossorigin="anonymous">
</head>
<body>
<div class="container">
<h1 class="text-center m-3">外気温統計情報</h1>
<div class="table-responsive-sm">
<table class="table table-sm table-bordered">
  <tbody>
  <tr>
    <th scope="row" class="table-light">当日測定値</th>
    <td colspan="2">{find_day}</td>
  </tr>
  <tr class="table-primary align-middle">
    <th scope="row" class="text-end">最低気温</th>
    <td class="align-middle text-center">{find_min_time}</td>
    <td class="text-end">{find_min_temper} ℃</td>
  </tr>
  <tr class="table-danger align-middle">
    <th scope="row" class="text-end">最高気温</th>
    <td class="align-middle text-center">{find_max_time}</td>
    <td class="text-end">{find_max_temper} ℃</td>
  </tr>
  </tbody>
</table>
</div>
<div class="table-responsive-sm">
<table class="table table-sm table-bordered">
  <tbody>
  <tr>
    <th scope="row">前日測定値</th>
    <td colspan="2">{before_day}</td>
  </tr>
  <tr class="table-primary align-middle">
    <th scope="row" class="text-end">最低気温</th>
    <td class="text-center">{before_min_time}</td>
    <td class="text-end">{before_min_temper} ℃</td>
  </tr>
  <tr class="table-danger align-middle">
    <th scope="row" class="text-end">最高気温</th>
    <td class="text-center">{before_max_time}</td>
    <td class="text-end">{before_max_temper} ℃</td>
  </tr>
  </tbody>
</table>
</div>
</div>
</body>
</html>
"""


# SQLite3 データベース接続オブジェクト取得関数
def get_connection(db_path: str, auto_commit=False, read_only=False, logger=None):
    connection: sqlite3.Connection
    try:
        if read_only:
            db_uri = "file://{}?mode=ro".format(db_path)
            connection = sqlite3.connect(db_uri, uri=True)
        else:
            connection = sqlite3.connect(db_path)
            if auto_commit:
                connection.isolation_level = None
    except sqlite3.Error as e:
        if logger is not None:
            logger.error(e)
        raise e
    return connection


# HTMLファイル保存関数
def save_html(file, contents):
    with open(file, 'w') as fp:
        fp.write(contents)


# ISO8601日付文字列+n日加算関数
def date_add_days(iso8601_date: str, add_days=1) -> str:
    dt: datetime = datetime.strptime(iso8601_date, "%Y-%m-%d")
    dt += timedelta(days=add_days)
    return dt.strftime("%Y-%m-%d")


# ISO8601日付文字列を日本語の年月日に変換する関数
def to_title_date(curr_date: str) -> str:
    dt: datetime = datetime.strptime(curr_date, "%Y-%m-%d")
    return dt.strftime("%Y 年 %m 月 %d 日")
