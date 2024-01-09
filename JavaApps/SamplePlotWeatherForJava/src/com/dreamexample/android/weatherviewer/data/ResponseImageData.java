package com.dreamexample.android.weatherviewer.data;

import com.google.gson.annotations.SerializedName;

import java.util.Base64;

import static com.dreamexample.android.weatherviewer.functions.MyLogging.DEBUG_OUT;

/**
 * 画像取得リクエストに対するレスポンス(RAWデータ)
 * Gsonライブラリで変換
 * [形式] JSON
 */
public class ResponseImageData {
    // 画像(png)のbase64エンコード文字列
    @SerializedName("img_src")
    private final String imgSrc;
    // 検索データ件数
    @SerializedName("rec_count")
    private final int recCount;

    public ResponseImageData(String imgSrc, int recCount) {
        this.imgSrc = imgSrc;
        this.recCount = recCount;
    }

    public byte[] getImageBytes() {
        if (this.imgSrc != null) {
            // img_src = "data:image/png;base64, ..base64string..."
            String[] datas = this.imgSrc.split(",");
            DEBUG_OUT.accept("ResponseImageData", "datas[0]" + datas[0]);
            return Base64.getDecoder().decode(datas[1]);
        } else {
            return null;
        }
    }

    public int getRecCount() { return recCount; }

    @Override
    public String toString() {
        return "ResponseImageData{" +
                "imgSrc.size='" + (imgSrc != null ? imgSrc.length() : 0) + '\'' +
                ", recCount=" + recCount +
                '}';
    }
}
