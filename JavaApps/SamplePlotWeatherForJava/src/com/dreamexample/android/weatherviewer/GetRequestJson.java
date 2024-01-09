package com.dreamexample.android.weatherviewer;

import android.content.res.AssetManager;
import android.util.Log;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Map;

public class GetRequestJson {
    private static final String LOG_TAG = "GetRequestJson";
    private static final String REQUEST_INFO_FILE = "request_info.json";


    public static void main(String[] args) throws FileNotFoundException {
        AssetManager am = new AssetManager();
        Gson gson = new Gson();
        Type typedMap = new TypeToken<Map<String, Map<String, String>>>() {
        }.getType();
        Log.d(LOG_TAG, "typedMap: " + typedMap);
        Map<String, Map<String, String>> map = gson.fromJson(
                new JsonReader(new InputStreamReader(am.open(REQUEST_INFO_FILE))), typedMap);
        Map<String, String> mRequestUrls = map.get("urls");
        Map<String, String> mRequestHeaders = map.get("headers");
        Log.d(LOG_TAG, "RequestUrls: " + mRequestUrls);
        Log.d(LOG_TAG, "RequestHeaders: " + mRequestHeaders);

    }
}
