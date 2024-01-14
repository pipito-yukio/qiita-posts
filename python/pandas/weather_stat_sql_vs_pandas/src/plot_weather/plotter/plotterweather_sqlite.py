import base64
from io import BytesIO
from datetime import datetime, timedelta
from typing import Dict, List

from pandas.core.frame import DataFrame

import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch
from matplotlib.pyplot import setp
from matplotlib.legend import Legend
from matplotlib.text import Text

from plot_weather.dataloader.tempout_loader_sqlite import (
    COL_TIME, COL_TEMP_OUT
)
from plot_weather.dataloader.pandas_statistics import (
    get_temp_out_stat, TempOutStat, TempOut
)

""" 気象データの外気温プロット画像のbase64エンコードテキストデータを出力する """

# 日本語表示
rcParams['font.family'] = ["sans-serif", "monospace"]
rcParams['font.sans-serif'] = ["IPAexGothic", "Noto Sans CJK JP"]
# 固定ピッチフォント
rcParams['font.monospace'] = ["Source Han Code JP", "Noto Sans Mono CJK JP"]
# カラー定数定義
COLOR_MIN_TEMPER: str = "darkcyan"
COLOR_MAX_TEMPER: str = "orange"


def sub_graph(ax: Axes, title: str, df: DataFrame, temp_stat: TempOutStat):
    def make_patch(label: str, temp_out: TempOut, patch_color: str) -> Patch:
        """ 指定されたラベルと外気温統計の凡例を生成 """
        # 発現時刻は時分
        patch_label: str = f"{label} {temp_out.temper:4.1f} ℃ [{temp_out.appear_time[11:16]}"
        return Patch(color=patch_color, label=patch_label)

    def plot_hline(axes: Axes, temper: float, line_color: str):
        """ 指定された統計情報の外気温の横線を生成する """
        line_style_dict: Dict = {"color": line_color, "linestyle": "dashed", "linewidth": 1.}
        axes.axhline(temper, **line_style_dict)

    # グリッド線
    ax.grid(linestyle="dotted", linewidth=1.0)
    # 軸ラベルのフォントサイズを小さめに設定
    setp(ax.get_xticklabels(), fontsize=9.)
    setp(ax.get_yticklabels(), fontsize=9.)
    # x軸フォーマット: 軸ラベルは時間 (00,03,06,09,12,15,18,21,翌日の00)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))
    # 外気温の最低(-20℃)と最大(40℃)
    ax.set_ylim(ymin=-20.0, ymax=40.0)
    # Y軸ラベル
    ax.set_ylabel("外気温 (℃)", fontsize=10.)

    # タイトルの測定日付
    curr_date: str = temp_stat.measurement_day
    # 最低気温と最高気温の凡例を生成
    mim_patch: Patch = make_patch("最低", temp_stat.min, COLOR_MIN_TEMPER)
    max_patch: Patch = make_patch("最高", temp_stat.max, COLOR_MAX_TEMPER)
    # 当日データx軸の範囲: 当日 00時 から 翌日 00時
    dt_min: datetime = datetime.strptime(curr_date, "%Y-%m-%d")
    dt_max: datetime = dt_min + timedelta(days=1)
    # タイトル日付
    title_date: str = dt_min.strftime("%Y 年 %m 月 %d 日")
    ax.set_xlim(xmin=dt_min, xmax=dt_max)
    # 外気温データプロット
    ax.plot(df[COL_TIME], df[COL_TEMP_OUT], color="blue", marker="")
    # 最低気温の横線
    plot_hline(ax, temp_stat.min.temper, COLOR_MIN_TEMPER)
    # 最高気温の横線
    plot_hline(ax, temp_stat.max.temper, COLOR_MAX_TEMPER)
    ax.set_title(f"【{title} データ】{title_date}")
    # 凡例の設定
    ax_legend: Legend = ax.legend(handles=[mim_patch, max_patch], fontsize=10.)
    # Patchオブジェクトのテキストラベルに日本語等倍フォントを設定する
    text: Text
    for text in ax_legend.get_texts():
        text.set_fontfamily("monospace")


def gen_plot_image(
        curr_df: DataFrame, before_df: DataFrame, phone_size: str = None) -> str:
    """
    観測データの画像を生成する
    """

    # 検索日の統計情報
    curr_stat: TempOutStat = get_temp_out_stat(curr_df)
    # 前日の統計情報
    before_stat: TempOutStat = get_temp_out_stat(before_df)

    # 端末に応じたサイズのプロット領域枠(Figure)を生成する
    fig: Figure
    if phone_size is not None and len(phone_size) > 8:
        sizes: List[str] = phone_size.split("x")
        width_pixel: int = int(sizes[0])
        height_pixel: int = int(sizes[1])
        density: float = float(sizes[2])
        # Androidスマホは pixel指定
        px: float = 1 / rcParams["figure.dpi"]  # pixel in inches
        px = px / (2.0 if density > 2.0 else density)
        fig_width_px: float = width_pixel * px
        fig_height_px: float = height_pixel * px
        fig = Figure(figsize=(fig_width_px, fig_height_px), constrained_layout=True)
    else:
        # PCブラウザはinch指定
        fig = Figure(figsize=(9.8, 6.4), constrained_layout=True)
    # ２行 (指定日データ, 前日データ) １列のサブプロット生成
    ax_temp_curr: Axes
    ax_temp_prev: Axes
    (ax_temp_curr, ax_temp_prev) = fig.subplots(nrows=2, ncols=1)

    # 1. 指定日の外気温プロット (上段)
    sub_graph(ax_temp_curr, "検索日", curr_df, curr_stat)
    # 2. 前日の外気温プロット (下段)
    sub_graph(ax_temp_prev, "前　日", before_df, before_stat)

    # 画像をバイトストリームに溜め込みそれをbase64エンコードしてレスポンスとして返す
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    return "data:image/png;base64," + data
