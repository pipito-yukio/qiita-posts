package com.dreamexample.android.weatherviewer.functions;

import android.util.Log;
import com.dreamexample.android.weatherdataviewer.BuildConfig;

import java.util.function.BiConsumer;

public class MyLogging {

    @FunctionalInterface
    public interface LogParamsConsumer<S, S1, T> {
        void print(String tag, String format, Object... params);
    }

    // DEBUG Log Function
    public static BiConsumer<String, String> DEBUG_OUT = (tag, output) -> {
        if (BuildConfig.DEBUG) {
            Log.d(tag, output);
        }
    };
    // DEBUG Log Function
    public LogParamsConsumer<String, String, Object[]> DEBUG_OUT_PARAMS = (tag, format, params) -> {
        if (BuildConfig.DEBUG) {
            String output = String.format(format, params);
            Log.d(tag, output);
        }
    };

}
