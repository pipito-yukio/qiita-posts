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
from matplotlib.legend import Legend
from matplotlib.text import Text
from matplotlib.pyplot import setp

from .pandas_statistics import COL_TIME, COL_TEMP_OUT

""" 
気象データの外気温プロット画像のbase64エンコードテキストデータを出力する
[凡例] 通常の統計情報線をそのま凡例に設定する
"""

# 日本語表示
rcParams['font.family'] = ["sans-serif", "monospace"]
rcParams['font.sans-serif'] = ["IPAexGothic", "Noto Sans CJK JP"]
# 固定ピッチフォント
rcParams['font.monospace'] = ["Source Han Code JP", "Noto Sans Mono CJK JP"]
# カラー定数定義
COLOR_MIN_TEMPER: str = "darkcyan"
COLOR_MAX_TEMPER: str = "orange"
COLOR_AVG_TEMPER: str = "red"


def make_graph(df_data: DataFrame) -> Figure:
    """ 観測データのDataFrameからグラフを生成し描画領域を取得する """

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

    # 図のタイトルに表示する測定日
    curr_date: str
    # 先頭のデータの測定時刻から測定日取得
    pd_timestamp: pd.Timestamp = df_data.iloc[0][COL_TIME]
    py_datetime: datetime = pd_timestamp.to_pydatetime()
    curr_date = py_datetime.strftime("%Y-%m-%d")
    # タイトル
    ax_temp.set_title(f"【測定日】{curr_date}")
    # Y軸ラベル
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
    ax_temp.plot(df_data[COL_TIME], df_data[COL_TEMP_OUT], color="blue", marker="")
    # 最低気温
    temper: float = round(df_data[COL_TEMP_OUT].min(), 1)
    ax_temp.axhline(temper, label=f"最低 {temper:4.1f} ℃",
                    color=COLOR_MIN_TEMPER, linestyle="dashed", linewidth=1.)
    # 最高気温
    temper = round(df_data[COL_TEMP_OUT].max(), 1)
    ax_temp.axhline(temper, label=f"最高 {temper:4.1f} ℃",
                    color="orange", linestyle="dashed", linewidth=1.)
    # 平均気温
    temper = round(df_data[COL_TEMP_OUT].mean(), 1)
    ax_temp.axhline(temper, label=f"平均 {temper:4.1f} ℃",
                    color="red", linestyle="dashdot", linewidth=1.)
    # 凡例に固定フォントを設定したい項目を追加
    ax_temp.legend(loc="best", title="外気温統計情報")
    # 数値を含むラベルに日本語等倍フォントを設定する
    ax_temp_legend: Legend = ax_temp.get_legend()
    text: Text
    for text in ax_temp_legend.get_texts():
        text.set_fontfamily("monospace")
    return fig


def gen_plot_image(csv_full_path: str) -> Tuple[int, Optional[str]]:
    """
    観測データの画像を生成する
    """
    df_data: pd.DataFrame = pd.read_csv(csv_full_path, parse_dates=[COL_TIME])
    if df_data.shape[0] == 0:
        # データなし
        return 0, None

    # 測定時刻列をインデックスに設定する
    df_data.index = df_data[COL_TIME]

    # 観測データサププロット生成
    # https://matplotlib.org/3.1.1/faq/howto_faq.html#how-to-use-matplotlib-in-a-web-application-server
    #  How to use Matplotlib in a web application server
    plot_figure: Figure = make_graph(df_data)
    # 画像をバイトストリームに溜め込む
    buf = BytesIO()
    plot_figure.savefig(buf, format="png", bbox_inches="tight")
    # バイトデータをbase64エンコード変換
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    # HTML img tag src
    img_src: str = "data:image/png;base64," + data
    # 件数とHTMLのimgタグに設定可能な文字列を返却
    return df_data.shape[0], img_src
