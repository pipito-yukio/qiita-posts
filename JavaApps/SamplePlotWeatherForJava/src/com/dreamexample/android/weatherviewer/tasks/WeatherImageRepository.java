package com.dreamexample.android.weatherviewer.tasks;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.dreamexample.android.weatherviewer.data.ResponseImageDataResult;

/**
 * 可視化画像取得リポジトリクラス
 */
public abstract class WeatherImageRepository extends WeatherRepository<ResponseImageDataResult> {
    public WeatherImageRepository() {}
    /**
     * 画像データ取得レスポンスはデータサイズが大きいのでバイトストリームバッファを経由して取得
     * @param is 入力ストリーム
     * @return レスボンス文字列
     */
    @Override
    public String getResponseText(InputStream is) throws IOException {
        String strJson = null;
        try(BufferedInputStream bufferedInput = new BufferedInputStream(is);
            ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // 画像データは8KB単位でバイトストリーム追加する
            byte[] buff = new byte[8 * 1024];
            int length;
            while ((length = bufferedInput.read(buff)) != -1) {
                out.write(buff, 0, length);
            }
            byte[] imgArray = out.toByteArray();
            if (imgArray.length > 0) {
                // 文字列に復元
                strJson = new String(imgArray, StandardCharsets.US_ASCII);
            }
        }
        return strJson;
    }

    /**
     * 画像データレスポンス
     *
     * @param jsonText JSON文字列
     * @return Gsonで変換したResponseImageResultオブジェクト
     */
    public ResponseImageDataResult parseResultJson(String jsonText) throws JsonParseException {
        Gson gson = new Gson();
        return gson.fromJson(jsonText, ResponseImageDataResult.class);
    }

    public abstract String getRequestPath(int urlIdx);

}
