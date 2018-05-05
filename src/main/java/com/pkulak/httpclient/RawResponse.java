package com.pkulak.httpclient;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.HttpResponseStatus;

public class RawResponse {
    private final HttpResponseStatus status;
    private final HttpHeaders headers;
    private final byte[] body;

    public RawResponse(HttpResponseStatus status, HttpHeaders headers, byte[] body) {
        this.status = status;
        this.headers = headers;
        this.body = body;
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

    public byte[] getBody() {
        return body;
    }
}

