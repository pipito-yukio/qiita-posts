package android.content.res;

import android.util.Log;

import java.io.*;
import java.net.URL;

/**
 * android.content.AssetManagerの擬似クラス
 * [res]ディレクトリのリクエスト用JSONファイルを読み込み、
 */
public class AssetManager {
    private static final String TAG = "AssetManager";

    private static final String RESOURCES_ROOT = "resources/";

    public AssetManager() {
    }

    public InputStream open(String filename) throws FileNotFoundException {
        File resFile = getResourceFile(filename);
        Log.d(TAG, "file: " + resFile);
        return new FileInputStream(resFile);
    }

    private File getResourceFile(final String fileName) throws FileNotFoundException {
        String resFile = RESOURCES_ROOT + fileName;
        URL url = this.getClass()
                .getClassLoader()
                .getResource(resFile);
        if (url != null) {
            return new File(url.getFile());
        }
        throw new FileNotFoundException(resFile + " not found!");
    }

}
