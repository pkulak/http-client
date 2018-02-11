package com.pkulak.httpclient.mapper;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

import java.util.function.Supplier;

/**
 * A response mapper that peeks at the status code, then closes the connection without waiting for headers or a body.
 */
public class StatusResponseHandler implements AsyncHandler<Integer> {
    private int status = 500;

    public static Supplier<StatusResponseHandler> supplier() {
        return StatusResponseHandler::new;
    }

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
        this.status = responseStatus.getStatusCode();
        return State.ABORT;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) throws Exception {
        return State.ABORT;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        return State.ABORT;
    }

    @Override
    public void onThrowable(Throwable t) {}

    @Override
    public Integer onCompleted() throws Exception {
        return status;
    }
}
