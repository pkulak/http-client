package com.pkulak.httpclient.response;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.HttpResponseStatus;

public class HeaderResponse {
    private final HttpResponseStatus status;
    private final HttpHeaders headers;

    public HeaderResponse(HttpResponseStatus status, HttpHeaders headers) {
        this.status = status;
        this.headers = headers;
    }

    public HttpResponseStatus getStatus() {
        return status;
    }

    public int getStatusCode() {
        return status.getStatusCode();
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return "HTTP/1.1 " + status;
    }
}
