package org.example.web;

public class ApiException extends RuntimeException {

    private final int status;

    public ApiException(int status, String detail) {
        super(detail);
        this.status = status;
    }

    public int status() {
        return status;
    }

    public String detail() {
        return getMessage();
    }
}
