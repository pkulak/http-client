package com.pkulak.httpclient.mapper;

import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

import java.io.IOException;

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

    public static class InvalidStatusException extends IOException {
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
}

