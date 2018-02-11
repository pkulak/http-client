package com.pkulak.httpclient.mapper;

import com.fasterxml.jackson.databind.ObjectReader;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * An {@link AsyncHandler} that uses a byte buffer when the response is small (under 2K) and streaming when otherwise
 * or unknown (chunked). The downside to streaming is that it requires a separate thread, but it doesn't need to buffer
 * the entire body before it can be processed.
 *
 * @param <T> the model type to return from the {@link com.pkulak.httpclient.HttpClient}
 */
public class JacksonResponseHandler<T> implements AsyncHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(JacksonResponseHandler.class);
    private static final int CUTOFF = 2048;

    private final ObjectReader reader;

    // use this if the content size is small
    private ByteBuffer buffer;

    // and these if the content size is large or unknown
    private PipedInputStream inputStream;
    private PipedOutputStream outputStream;
    private int totalBytes;
    private T model;
    private CountDownLatch latch;
    private final ExecutorService executor;

    public JacksonResponseHandler(ObjectReader reader, ExecutorService executor) {
        this.reader = reader;
        this.executor = executor;
    }

    public static <U> Supplier<JacksonResponseHandler<U>> supplier(ObjectReader reader, ExecutorService executor) {
        return () -> new JacksonResponseHandler<U>(reader, executor);
    }

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) throws Exception {
        if (headers.contains(HttpHeaderNames.CONTENT_LENGTH)) {
            int length = Integer.parseInt(headers.get(HttpHeaderNames.CONTENT_LENGTH));

            if (length < CUTOFF) {
                setupForBuffering(length);
            } else {
                setupForStreaming();
            }
        } else {
            setupForStreaming();
        }

        return State.CONTINUE;
    }

    private void setupForBuffering(int size) {
        buffer = ByteBuffer.allocate(size);
    }

    private void setupForStreaming() throws IOException {
        latch = new CountDownLatch(1);
        inputStream = new PipedInputStream();
        outputStream = new PipedOutputStream(inputStream);

        executor.execute(() -> {
            try {
                model = reader.readValue(inputStream);
            } catch (IOException e) {
                if (totalBytes == 0) {
                    log.warn("Expecting a body, but none was returned. Maybe set statusOnly() on the client?");
                } else {
                    throw new UncheckedIOException(e);
                }
            } finally {
                latch.countDown();
            }
        });
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        if (buffer != null) {
            buffer.put(bodyPart.getBodyByteBuffer());
        } else {
            totalBytes += bodyPart.length();
            outputStream.write(bodyPart.getBodyPartBytes());
        }

        return State.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) {}

    @Override
    public T onCompleted() throws Exception {
        if (buffer != null) {
            if (buffer.position() == 0) {
                return null;
            }

            return reader.readValue(buffer.array());
        } else {
            outputStream.close();
            latch.await();
            return model;
        }
    }
}
