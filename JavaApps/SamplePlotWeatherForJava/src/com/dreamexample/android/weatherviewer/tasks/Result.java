package com.dreamexample.android.weatherviewer.tasks;


import com.dreamexample.android.weatherviewer.data.ResponseStatus;

public abstract class Result<T> {
    private Result() {
    }

    public static final class Success<T> extends Result<T> {
        private final T data;

        public Success(T data) {
            this.data = data;
        }

        public T get() {return data; }
    }

    public static final class Warning<T> extends Result<T> {
        private final ResponseStatus status;

        public Warning(ResponseStatus status) {
            this.status = status;
        }

        public ResponseStatus getResponseStatus() { return status; }
    }

    public static final class Error<T> extends Result<T> {
        private final Exception exception;

        public Error(Exception exception) {
            this.exception = exception;
        }

        public Exception getException() { return exception; }
    }
}
