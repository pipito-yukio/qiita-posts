import os
from typing import List, Optional

from plot_weather.plotter.plotterweather_basicLenend import gen_plot_image

"""
外気温データ(CSVファイル)の最低気温と最高気温を出現時刻とともプロット画像に出力する
[凡例] 一番シンプルな凡例 ※統計情報なし
"""

# スクリプト名
script_name = os.path.basename(__file__)
# CSVファイル
CSV_PATH: str = os.path.join("data", "t_weather_temp_out.csv")

# 出力画層用HTMLテンプレート
OUT_HTML = """
<!DOCTYPE html>
<html lang="ja">
<body>
<img src="{}" alt="外気温データプロット画像" border="1" />
</body>
</html>
"""


def save_text(file, contents):
    with open(file, 'w') as fp:
        fp.write(contents)


if __name__ == '__main__':
    rec_count: int
    img_src: Optional[str]
    rec_count, img_src = gen_plot_image(CSV_PATH)
    if rec_count > 0:
        # 出力結果をHTMLテンプレートに設定する
        script_names: List[str] = script_name.split(".")
        save_name = f"{script_names[0]}.html"
        save_path = os.path.join("output", save_name)
        html: str = OUT_HTML.format(img_src)
        save_text(save_path, html)
        print(f"Saved {save_path}")
    else:
        print("TempOut record not found.")
