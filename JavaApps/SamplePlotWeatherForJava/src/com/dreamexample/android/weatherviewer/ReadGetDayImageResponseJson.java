package com.dreamexample.android.weatherviewer;

import com.google.gson.Gson;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.dreamexample.android.weatherviewer.data.ResponseImageData;
import com.dreamexample.android.weatherviewer.data.ResponseImageDataResult;

import static com.dreamexample.android.weatherviewer.functions.MyLogging.DEBUG_OUT;

public class ReadGetDayImageResponseJson {
    private static final String TAG = "ReadPlotWeatherResponseJson";
    static final String USER_HOME = System.getProperty("user.home");
    static final Path JSON_PATH = Paths.get(USER_HOME, "Documents", "PlotWeather", "json");
    static final String OUTPUT_PATH = Paths.get(
            USER_HOME, "Documents", "output").toString();

    public static void main(String[] args) {
        String jsonName = args[0];
        String jsonFullPath = Paths.get(JSON_PATH.toString(), jsonName).toString();
        List<String> lines = new ArrayList<>();
        try  {
            FileInputStream fis = new FileInputStream(jsonFullPath);
            InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
            try (BufferedReader reader = new BufferedReader(inputStreamReader))  {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line + '\n');
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("IOException: " + e.getLocalizedMessage());
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String responseJson = String.join("", lines);
        try {
            Gson gson = new Gson();
            ResponseImageDataResult respObj = gson.fromJson(responseJson, ResponseImageDataResult.class);
            ResponseImageData data = respObj.getData();
            if (data.getRecCount() > 0) {
                byte[] decoded = data.getImageBytes();
                String saveName = jsonName.replace(".json", ".png");
                String saveFilePath = Paths.get(OUTPUT_PATH, saveName).toString();
                try (FileOutputStream out = new FileOutputStream(saveFilePath)) {
                    out.write(decoded);
                    DEBUG_OUT.accept(TAG, String.format("%s を保存しました", saveFilePath));
                } catch (IOException exp) {
                    // No ope
                }
            } else {
                DEBUG_OUT.accept(TAG,"気象データ無し");
            }
        } catch (Exception e) {
            // パースエラー
            System.out.println("Exception: " + e.getLocalizedMessage());
        }

    }
}
