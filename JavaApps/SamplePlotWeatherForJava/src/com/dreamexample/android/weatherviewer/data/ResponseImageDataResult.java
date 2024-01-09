package com.dreamexample.android.weatherviewer.data;

/**
 * 画像取得リクエスト用レスポンスクラス
 */
public class ResponseImageDataResult {
    private final ResponseImageData data;
    private final ResponseStatus status;

    public ResponseImageDataResult(ResponseImageData data, ResponseStatus status) {
        this.data = data;
        this.status = status;
    }

    public ResponseImageData getData() { return this.data; }

    public ResponseStatus getStatus() {
        return status;
    }

}
