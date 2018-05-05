package com.pkulak.httpclient.mapper;

import com.pkulak.httpclient.InvalidStatusException;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

/**
 * A very simple response mapper that only throws when something goes wrong. None of the response is read after the
 * status line, unless it's an error status (since the body may be useful for debugging in that case).
 */
public class VoidResponseMapper implements AsyncHandler<Void> {
    private HttpResponseStatus responseStatus;
    private StringResponseMapper stringResponseMapper;

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) {
        if (isErrorResponseCode(responseStatus)) {
            this.responseStatus = responseStatus;
            stringResponseMapper = new StringResponseMapper();
            return State.CONTINUE;
        }

        return State.ABORT;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) {
        return State.CONTINUE;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        stringResponseMapper.onBodyPartReceived(bodyPart);
        return State.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) {}

    @Override
    public Void onCompleted() throws Exception {
        if (stringResponseMapper != null) {
            throw new InvalidStatusException(responseStatus, stringResponseMapper.onCompleted());
        }

        return null;
    }

    public static boolean isErrorResponseCode(HttpResponseStatus status) {
        return status.getStatusCode() >= 300;
    }

}

