package com.dreamexample.android.weatherviewer.tasks;

import static com.dreamexample.android.weatherviewer.functions.MyLogging.DEBUG_OUT;

import android.os.Handler;
import android.util.Log;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.dreamexample.android.weatherviewer.data.ResponseStatus;
import com.dreamexample.android.weatherviewer.data.ResponseWarningStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class WeatherRepository<T> {
    private static final String TAG = "WeatherRepository";

    public WeatherRepository() {}

    /**
     * GETリクエスト生成メソッド
     * @param pathIdx パス用インデックス ※サブクラスで複数のGETリクエストパス定義
     * @param baseUrl パスを含まないURL (Wifi | Mobile)
     * @param requestParameter リクエストパラメータ (null可)
     * @param headers リクエストヘッダー
     * @param executor スレッドエクゼキューター
     * @param handler Android Handlerオブジェクト
     * @param callback Activity(Fragment)が結果を受け取るコールバック
     */
    public void makeGetRequest(
            int pathIdx,
            String baseUrl,
            String requestParameter,
            Map<String, String> headers,
            ExecutorService executor,
            Handler handler,
            final RepositoryCallback<T> callback) {
        executor.execute(() -> {
            try {
                String requestUrl = baseUrl + getRequestPath(pathIdx)
                        + (requestParameter != null ? requestParameter: "");
                DEBUG_OUT.accept(TAG, "requestUrl:" + requestUrl);
                Result<T> result = getRequest(requestUrl, headers);
                // 200, 4xx - 50x系
                notifyResult(result, callback, handler);
            } catch (Exception e) {
                // サーバー側のレスポンスBUGか, Android側のBUG想定
                Result<T> errorResult = new Result.Error<>(e);
                notifyResult(errorResult, callback, handler);
            }
        });
    }

    private void notifyResult(final Result<T> result,
                              final RepositoryCallback<T> callback,
                              final Handler handler) {
        handler.post(() -> callback.onComplete(result));
    }

    private Result<T> getRequest(
            String requestUrl, Map<String, String> requestHeaders) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(requestUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json;");
            for (String key : requestHeaders.keySet()) {
                conn.setRequestProperty(key, requestHeaders.get(key));
            }

            // Check response code: allow 200 only.
            int respCode = conn.getResponseCode();
            DEBUG_OUT.accept(TAG, "ResponseCode:" + respCode);
            if (respCode == HttpURLConnection.HTTP_OK) {
                String respText = getResponseText(conn.getInputStream());
                T result = parseResultJson(respText);
                return new Result.Success<>(result);
            } else {
                // 4xx - 50x
                // Flaskアプリからはエラーストリームが生成される
                String respText = getResponseText(conn.getErrorStream());
                DEBUG_OUT.accept(TAG, "NG.Response.JSON: \n" + respText);
                // ウォーニング時のJSONはデータ部が存在しないのでウォーニング専用ハースを実行
                ResponseStatus status = getWarningStatus(respText);
                return new Result.Warning<>(status);
            }
        } catch (Exception ie) {
            Log.w(TAG, ie.getLocalizedMessage());
            return new Result.Error<>(ie);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * 入力ストリームからJSON文字列を取得
     * @param is 入力ストリーム
     * @return JSON文字列
     * @throws IOException IO例外
     */
    public String getResponseText(InputStream is) throws IOException {
        StringBuilder sb;
        try (BufferedReader bf = new BufferedReader
                (new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            sb = new StringBuilder();
            while ((line = bf.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    /**
     * ウォーニング時のレスポンスオブジェクトを取得する
     * <pre>ウォーニング時にサーバーが返却するレスポンス例
     {"status": {"code": 400,"message": "461,User is not found."}}
     * </pre>
     * @param jsonText ウォーニング用JSON文字列
     * @return レスポンスオブジェクト<br/>
     *   ResponseStatusオブジェクトのみがセットされDataオブジェクトはnullがセットされる
     * @throws JsonParseException パース例外
     */
    public ResponseStatus getWarningStatus(String jsonText) throws JsonParseException {
        Gson gson = new GsonBuilder().serializeNulls().create();
        ResponseWarningStatus warningStatus = gson.fromJson(jsonText, ResponseWarningStatus.class);
        return warningStatus.getStatus();
    }

    /**
     * リクエストパスを取得する
     * @param pathIdx パスインデックス (1 - m)
     * @return サブクラスが提供するパス
     */
    public abstract String getRequestPath(int pathIdx);

    /**
     * HTTP 200(OK) レスポンス時のJSON文字列をパースしてJavaオブジェクトを生成
     * @param jsonText JSON文字列を
     * @return サブグラスが定義するJavaオブジェクトを生成
     * @throws JsonParseException GSONのパース例外
     */
    public abstract T parseResultJson(String jsonText) throws JsonParseException;

}
