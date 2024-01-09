package com.dreamexample.android.weatherviewer.data;

public class ResponseStatus {
    private final int code;
    private final String message;

    public ResponseStatus(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() { return code; }

    public String getMessage() { return message; }

    @Override
    public String toString() {
        return "ResponseStatus{" +
                "code=" + code +
                ", message='" + message + '\'' +
                '}';
    }
}
