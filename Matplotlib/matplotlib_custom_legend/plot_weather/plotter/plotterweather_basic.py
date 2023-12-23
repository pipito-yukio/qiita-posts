import base64
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional, Tuple

import pandas as pd
from pandas.core.frame import DataFrame

import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.pyplot import setp

""" 
気象データの外気温プロット画像のbase64エンコードテキストデータを出力する
凡例の無いデータをプロットしただけのグラフ
[前提条件]
  Webアプリで使用するモジュールが前提のため matplotlib.pyplot でGUI表示せずにプロット画像をHTML出力する
  最低気温、最高気温は１日で複数回出現する可能があるので取得する最低・最高気温は直近の値とする
  日本語フォントがインストール済み
"""

# 日本語表示
rcParams['font.family'] = ["sans-serif", "monospace"]
rcParams['font.sans-serif'] = ["IPAexGothic", "Noto Sans CJK JP"]
# カラム情報
COL_TIME: str = "measurement_time"
COL_TEMP_OUT: str = "temp_out"


def gen_plot_image(csv_full_path: str) -> Tuple[int, Optional[str]]:
    """
    観測データの画像を生成する
    """
    df_data: DataFrame = pd.read_csv(csv_full_path, parse_dates=[COL_TIME])
    if df_data.shape[0] == 0:
        # データなし
        return 0, None

    # 測定時刻列をインデックスに設定する
    df_data.index = df_data[COL_TIME]

    # 観測データプロットグラフ生成
    # 図の生成
    px: float = 1 / rcParams["figure.dpi"]  # pixel in inches
    fig_width_px: float = 600 * px
    fig_height_px: float = 480 * px
    fig = Figure(figsize=(fig_width_px, fig_height_px), constrained_layout=True)

    # 外気温サププロット領域生成
    ax_temp: Axes = fig.subplots(nrows=1, ncols=1)
    # グリッド線設定 ※x軸,y軸
    ax_temp.grid(linestyle="dotted", linewidth=1.0)

    # 軸ラベルのフォントサイズを設定
    setp(ax_temp.get_xticklabels(), fontsize=9.)
    setp(ax_temp.get_yticklabels(), fontsize=9.)

    # 図のタイトルに表示する測定日をデータの先頭から取得
    pd_timestamp: pd.Timestamp = df_data.iloc[0][COL_TIME]
    print(f"pd_timestamp: {type(pd_timestamp)}")
    py_datetime: datetime = pd_timestamp.to_pydatetime()
    curr_date: str = py_datetime.strftime("%Y-%m-%d")
    # 図のタイトル
    ax_temp.set_title(f"【測定日】{curr_date}")
    # y軸ラベル
    ax_temp.set_ylabel("外気温 (℃)", fontsize=10.)

    # x軸の範囲: 当日 00時 から 翌日 00時
    dt_curr: datetime = datetime.strptime(curr_date, "%Y-%m-%d")
    dt_next: datetime = dt_curr + timedelta(days=1)
    ax_temp.set_xlim(xmin=dt_curr, xmax=dt_next)
    # x軸フォーマット: 軸ラベルは時間 (00,03,06,09,12,15,18,21,翌日の00)
    ax_temp.xaxis.set_major_formatter(mdates.DateFormatter("%H"))
    # 外気温の範囲: 最低(-20℃)〜最大(40℃)
    ax_temp.set_ylim(ymin=-20.0, ymax=40.0)

    #  外気温のプロット
    ax_temp.plot(
        df_data[COL_TIME], df_data[COL_TEMP_OUT], color="blue", marker=""
    )
    # 画像をバイトストリームに溜め込む
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    # バイトデータをbase64エンコード変換
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    # HTML img tag src
    img_src: str = "data:image/png;base64," + data
    # 件数とHTMLのimgタグに設定可能な文字列を返却
    return df_data.shape[0], img_src
