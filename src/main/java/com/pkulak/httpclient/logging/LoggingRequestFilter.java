package com.pkulak.httpclient.logging;

import com.google.common.io.CharStreams;
import org.asynchttpclient.Request;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.RequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class LoggingRequestFilter implements RequestFilter {
    private static final Logger log = LoggerFactory.getLogger(LoggingRequestFilter.class);

    @Override
    public <T> FilterContext<T> filter(FilterContext<T> ctx) {
        if (!log.isTraceEnabled()) return ctx;

        StringBuilder builder = new StringBuilder("\n");
        Request req = ctx.getRequest();

        builder.append(req.getMethod())
                .append(" ")
                .append(req.getUrl())
                .append("\n");

        req.getHeaders().forEach(entry ->
                builder.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"));

        if (req.getStreamData() != null) {
            builder.append("\n");
            builder.append(streamToString(req.getStreamData()));
        }

        builder.append("\n");

        log.trace(builder.toString());

        return ctx;
    }

    // do our best to turn streams to strings using a whitelist of implementations that are seekable.
    private String streamToString(InputStream in) {
        if (in instanceof ByteArrayInputStream) {
            try {
                String body = CharStreams.toString(new InputStreamReader(in));
                in.reset();
                return body;
            } catch (IOException e) {
                return e.getMessage();
            }
        }

        return in.toString();
    }
}
