package com.pkulak.httpclient;

import org.asynchttpclient.HttpResponseStatus;

import java.io.IOException;

public class InvalidStatusException extends IOException {
    private HttpResponseStatus status;
    private String body;

    public InvalidStatusException(HttpResponseStatus status, String body) {
        super(String.format("invalid response status (%d) from %s: %s",
                status.getStatusCode(),
                status.getUri().toString(),
                body));

        this.status = status;
        this.body = body;
    }

    public HttpResponseStatus getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }
}
