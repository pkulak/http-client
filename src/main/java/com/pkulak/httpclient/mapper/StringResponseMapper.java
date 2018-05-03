package com.pkulak.httpclient.mapper;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

public class StringResponseMapper implements AsyncHandler<String> {
    private StringBuilder builder = new StringBuilder();

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) {
        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) {
        return State.CONTINUE;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        builder.append(new String(bodyPart.getBodyPartBytes(), "UTF-8"));
        return State.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) {}

    @Override
    public String onCompleted() {
        return builder.toString();
    }
}

