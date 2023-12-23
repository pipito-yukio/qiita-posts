import base64
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, Optional, Tuple

import pandas as pd
from pandas.core.frame import DataFrame

import matplotlib.dates as mdates
from matplotlib import rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Patch
from matplotlib.pyplot import setp
from matplotlib.legend import Legend
from matplotlib.text import Text

from .pandas_statistics import (
    COL_TIME, COL_TEMP_OUT, TempOutStat, TempOut, get_temp_out_stat
)

""" 
気象データの外気温プロット画像のbase64エンコードテキストデータを出力する
[凡例] Patchオブジェクト使用
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


def make_graph(df_data: DataFrame, temp_out_stat: TempOutStat) -> Figure:
    """ 観測データのDataFrameからグラフを生成し描画領域を取得する """

    def make_patch(label: str, temper: float, patch_color: str, appear_time: Optional[str]) -> Patch:
        """ 指定されたラベルと外気温統計の凡例を生成 """

        if appear_time is not None:
            # 最低気温と最高気温は出現時刻を含む
            patch_label: str = f"{label} {temper:4.1f} ℃ [{appear_time}]"
        else:
            # 平均気温は出現時刻なし
            patch_label: str = f"{label} {temper:4.1f} ℃"
        return Patch(color=patch_color, label=patch_label)

    def plot_hline(axes: Axes, temper: float, line_color: str, line_style):
        """ 指定された統計情報の外気温の横線を生成する """
        # axhilne()に引き渡すパラメータが長くなるので線種情報はDictに纏めて設定する
        line_style_dict: Dict = {"color": line_color, "linestyle": line_style, "linewidth": 1.}
        axes.axhline(temper, **line_style_dict)

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
    curr_date: str = temp_out_stat.measurement_day
    # タイトル
    ax_temp.set_title(f"【測定日】{curr_date}")
    # Y軸ラベル
    ax_temp.set_ylabel("外気温 (℃)", fontsize=10.)

    # 当日データx軸の範囲: 当日 00時 から 翌日 00時
    dt_curr: datetime = datetime.strptime(curr_date, "%Y-%m-%d")
    dt_next: datetime = dt_curr + timedelta(days=1)
    ax_temp.set_xlim(xmin=dt_curr, xmax=dt_next)
    # x軸フォーマット: 軸ラベルは時間 (00,03,06,09,12,15,18,21,翌日の00)
    ax_temp.xaxis.set_major_formatter(mdates.DateFormatter("%H"))
    # 外気温の範囲: 最低(-20℃)〜最大(40℃)
    ax_temp.set_ylim(ymin=-20.0, ymax=40.0)

    #  外気温のプロット
    ax_temp.plot(df_data[COL_TIME], df_data[COL_TEMP_OUT], color="blue", marker="")
    # 凡例に追加する最低・最高・平均気温の統計情報(Patch)を生成する
    stat_min: TempOut = temp_out_stat.min
    min_temper: float = round(stat_min.temper, 1)
    mim_patch: Patch = make_patch("最低", min_temper, COLOR_MIN_TEMPER,
                                  appear_time=stat_min.appear_time)
    stat_max: TempOut = temp_out_stat.max
    max_temper: float = round(stat_max.temper, 1)
    max_patch: Patch = make_patch("最高", max_temper, COLOR_MAX_TEMPER,
                                  appear_time=stat_max.appear_time)
    avg_temper: float = round(temp_out_stat.average_temper, 1)
    avg_patch: Patch = make_patch("平均", avg_temper, COLOR_AVG_TEMPER,
                                  appear_time=None)
    # 横線: 最低気温
    plot_hline(ax_temp, min_temper, COLOR_MIN_TEMPER, line_style="dashed")
    # 横線: 最高気温
    plot_hline(ax_temp, max_temper, COLOR_MAX_TEMPER, line_style="dashed")
    # 横線: 平均気温
    plot_hline(ax_temp, avg_temper, COLOR_AVG_TEMPER, line_style="dashdot")
    # 凡例の設定: パッチオブジェクトを追加する
    ax_temp_legend: Legend = ax_temp.legend(
        handles=[mim_patch, max_patch, avg_patch], title="外気温統計情報"
    )
    # ax_temp_legend.get_title().set_color("red")
    # その２
    # ax_temp_legend.get_frame().set_edgecolor("dimgray")
    # ax_temp_legend.get_frame().set_facecolor("lemonchiffon")
    # 凡例の全てのテキストに日本語の固定フォントを設定する
    # https://matplotlib.org/stable/users/explain/text/text_props.html
    # rcParams.update({"font.family": "monospace"})
    text: Text
    for text in ax_temp_legend.get_texts():
        # 日本語等倍フォント
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
    # 直近の最低と最高気温を取得するために降順にソートする ※データは測定時刻順 (昇順)
    # Default inplace=False is new DataFrame.
    sorted_df: pd.DataFrame = df_data.sort_index(ascending=False)
    # 外気温の統計情報を取得
    stat: TempOutStat = get_temp_out_stat(sorted_df)

    # 観測データプロットグラフ生成
    plot_figure: Figure = make_graph(df_data, stat)
    # 画像をバイトストリームに溜め込む
    buf = BytesIO()
    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.savefig.html
    #  matplotlib.pyplot.savefig
    plot_figure.savefig(buf, format="png", bbox_inches="tight", pad_inches=.2)
    # バイトデータをbase64エンコード変換
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    # HTML img tag src
    img_src: str = "data:image/png;base64," + data
    # 件数とHTMLのimgタグに設定可能な文字列を返却
    return df_data.shape[0], img_src
