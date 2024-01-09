package android.util;

/**
 * android.util.Logの擬似クラス
 * System.out.printlin()でコンソールに出力
 */
public class Log {
    private Log() {}
    public static void d(String tag, String message) {
        System.out.println("D/" + tag + ":" + message);
    }
    public static void w(String tag, String message) {
        System.out.println("W/" + tag + ":" + message);
    }
    public static void e(String tag, String message) {
        System.out.println("E/" + tag + ":" + message);
    }
}
