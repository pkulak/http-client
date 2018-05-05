package com.pkulak.httpclient.logging;

import com.pkulak.httpclient.mapper.JacksonResponseMapper;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.ResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class LoggingResponseFilter implements ResponseFilter {
    private static final Logger log = LoggerFactory.getLogger(LoggingResponseFilter.class);

    @Override
    public <T> FilterContext<T> filter(FilterContext<T> ctx) {
        if (log.isTraceEnabled()) {
            if (ctx.getAsyncHandler() instanceof BodyListenable) {
                ((BodyListenable) ctx.getAsyncHandler()).setBodyListener(new LoggingListener<T>(ctx));
            } else {
                log.trace("HTTP/1.1 " + ctx.getResponseStatus().toString() + "\n");
            }
        }

        return ctx;
    }

    private static class LoggingListener<T> implements BodyListener {
        private final StringBuilder builder = new StringBuilder("\n");

        private LoggingListener(FilterContext<T> ctx) {
            builder.append("HTTP/1.1 ").append(ctx.getResponseStatus().toString()).append("\n");

            ctx.getResponseHeaders().forEach(entry ->
                    builder.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"));

            builder.append("\n");
        }

        @Override
        public void onBodyPartReceived(HttpResponseBodyPart bodyPart) {
            try {
                builder.append(new String(bodyPart.getBodyPartBytes(), "UTF-8"));
            } catch (UnsupportedEncodingException e) { /* oh well */ }
        }

        @Override
        public void onRequestComplete() {
            builder.append("\n");
            log.trace(builder.toString());
        }
    }
}

