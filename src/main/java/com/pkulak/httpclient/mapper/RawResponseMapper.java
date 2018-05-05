package com.pkulak.httpclient.mapper;

import com.pkulak.httpclient.response.FullResponse;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

import java.io.ByteArrayOutputStream;

/**
 * A convenience response mapper for when you want all the details about the
 * response without any mapping.
 */
public class RawResponseMapper implements AsyncHandler<FullResponse> {
    private HttpResponseStatus status;
    private HttpHeaders headers;
    private ByteArrayOutputStream body = new ByteArrayOutputStream();

    @Override
    public State onStatusReceived(HttpResponseStatus status) {
        this.status = status;
        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) {
        this.headers = headers;
        return State.CONTINUE;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) {
        body.write(bodyPart.getBodyPartBytes(), 0, bodyPart.length());
        return State.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) { }

    @Override
    public FullResponse onCompleted() {
        return new FullResponse(status, headers, body.toByteArray());
    }
}

