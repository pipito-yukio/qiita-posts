package com.dreamexample.android.weatherviewer;

import static com.dreamexample.android.weatherviewer.functions.MyLogging.DEBUG_OUT;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import android.util.Log;

import com.dreamexample.android.weatherviewer.data.ResponseImageData;
import com.dreamexample.android.weatherviewer.data.ResponseImageDataResult;
import com.dreamexample.android.weatherviewer.data.ResponseStatus;
import com.dreamexample.android.weatherviewer.tasks.Result;
import com.dreamexample.android.weatherviewer.tasks.WeatherGraphRepository;


/**
 * 当日気象データ画像取得
 */
public class MainGetDayImage {
    static final String TAG = "MainGetGraph";
    static final String RESPONSE_FILE = "getTodayImage_%s.png";
    static final String USER_HOME = System.getenv("HOME");
    static final String OUTPUT_PATH = Paths.get(
            USER_HOME, "Documents", "output").toString();

    public static void main(String[] args) {
        // 実行時の引数に、デバイス名, 検索日, '幅x高さx密度' 順で設定する
        // (1) デバイス名
        String deviceName = args[0];
        // (2) 検索日: ISO8601形式 (例) 2023-12-31
        String findDate = args[1];
        // (3) 画像領域サイズ (例) '1064x1680x2.5'
        String argPhoneImageSize = args[2];

        WeatherApplication app = new WeatherApplication();
        WeatherGraphRepository repos = new WeatherGraphRepository();
        // ローカルネットワーク(Wi-Fi)のリクエストURL取得
        String requestUrl = app.getRequestUrls().get("wifi");
        // リクエストヘッダーに画像領域サイズを設定
        Map<String, String> headers = app.getRequestHeaders();
        headers.put(WeatherApplication.REQUEST_IMAGE_SIZE_KEY, argPhoneImageSize);
        // リクエストパラメータ: '/<device_name>/<find_date>'
        String requestParam = String.format("/%s/%s", deviceName, findDate);
        try {
            repos.makeGetRequest(0, requestUrl, requestParam, headers,
                    app.mEexecutor, app.mHandler, (result) -> {
                if (result instanceof Result.Success) {
                    // 正常レスポンス受信
                    ResponseImageDataResult imageResult =
                            ((Result.Success<ResponseImageDataResult>) result).get();
                    ResponseImageData data = imageResult.getData();
                    DEBUG_OUT.accept(TAG, "data: " + data);
                    if (data.getRecCount() > 0) {
                        byte[] decoded = data.getImageBytes();
                        String saveName = String.format(RESPONSE_FILE, findDate);
                        String saveFilePath = Paths.get(OUTPUT_PATH, saveName).toString();
                        try (FileOutputStream out = new FileOutputStream(saveFilePath)) {
                            out.write(decoded);
                        } catch (IOException exp) {
                            // No ope
                        }
                        DEBUG_OUT.accept(TAG, String.format("%s を保存しました", saveFilePath));
                    } else {
                        DEBUG_OUT.accept(TAG, String.format("'%s'の気象データ無し", deviceName));
                    }
                } else if (result instanceof Result.Warning) {
                    // エラーレスポンス受信
                    ResponseStatus status =
                            ((Result.Warning<?>) result).getResponseStatus();
                    DEBUG_OUT.accept(TAG, "WarningStatus: " + status);
                } else if (result instanceof Result.Error) {
                    // リクエストまたはレスポンス受信処理で例外発生
                    Exception exception = ((Result.Error<?>) result).getException();
                    Log.w(TAG, "GET error:" + exception.toString());
                }
            });
        } finally {
            // Javaアプリでは一回きりの実行なのでシャットダウンでプロセスを終了させる
            app.mEexecutor.shutdownNow();
        }
    }
}
