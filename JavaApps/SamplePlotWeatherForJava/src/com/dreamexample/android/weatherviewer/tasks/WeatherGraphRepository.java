package com.dreamexample.android.weatherviewer.tasks;

/**
 * GraphFragment用データ取得リポジトリ
 */
public class WeatherGraphRepository extends WeatherImageRepository {
    /** 指定日の気象データ画像取得[0], 本日から指定日前[1] */
    private static final String[] URL_PATHS = {"/getdayimageforphone"};

    public WeatherGraphRepository() {}

    @Override
    public String getRequestPath(int pathIdx) {
        return URL_PATHS[pathIdx];
    }

}
