package androidx.core.os;

import android.os.Handler;
import android.os.Looper;

public class HandlerCompat {
    public static Handler createAsync(Looper lopper) {
        return new Handler();
    }

}
