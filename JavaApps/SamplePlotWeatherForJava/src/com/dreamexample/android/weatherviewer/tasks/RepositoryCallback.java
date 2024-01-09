package com.dreamexample.android.weatherviewer.tasks;

public interface RepositoryCallback<T> {
    void onComplete(Result<T> result);
}
