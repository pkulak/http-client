package com.pkulak.httpclient.mapper;

import com.pkulak.httpclient.RawResponse;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

public class HeaderResponseMapper implements AsyncHandler<RawResponse> {
    private HttpResponseStatus status;
    private HttpHeaders headers;

    @Override
    public State onStatusReceived(HttpResponseStatus status) throws Exception {
        this.status = status;
        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) throws Exception {
        this.headers = headers;
        return State.ABORT;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        return State.ABORT;
    }

    @Override
    public void onThrowable(Throwable t) {

    }

    @Override
    public RawResponse onCompleted() throws Exception {
        return new RawResponse(status, headers, new byte[] {});
    }
}
