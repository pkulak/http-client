package com.pkulak.httpclient.response;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.HttpResponseStatus;

public class FullResponse extends HeaderResponse {
    private final byte[] body;

    public FullResponse(HttpResponseStatus status, HttpHeaders headers, byte[] body) {
        super(status, headers);
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }
}

