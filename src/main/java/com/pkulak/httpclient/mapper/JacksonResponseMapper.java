package com.pkulak.httpclient.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * An {@link AsyncHandler} that uses a byte buffer when the response is small (under 1K) or unknown (chunked) and
 * streaming otherwise. The downside to streaming is that it requires a separate thread, but it doesn't need to buffer
 * the entire body before it can be processed.
 *
 * @param <T> the model type to return from the {@link com.pkulak.httpclient.HttpClient}
 */
public class JacksonResponseMapper<T> implements AsyncHandler<T> {
    private static final int CUTOFF = 2048;

    private final ObjectMapper mapper;
    private final Class<T> modelType;

    // use this if the content size is small
    private ByteBuffer buffer;

    // and these if the content size is large or unknown
    private PipedInputStream inputStream;
    private PipedOutputStream outputStream;
    private T model;
    private CountDownLatch latch;
    private final ExecutorService executor;

    public JacksonResponseMapper(ObjectMapper mapper, Class<T> modelType, ExecutorService executor) {
        this.mapper = mapper;
        this.modelType = modelType;
        this.executor = executor;
    }

    public static <U> Supplier<JacksonResponseMapper<U>> supplier(
            ObjectMapper mapper, Class<U> modelType, ExecutorService executor) {
        return () -> new JacksonResponseMapper<>(mapper, modelType, executor);
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
                model = mapper.readValue(inputStream, modelType);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
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

            return mapper.readValue(buffer.array(), modelType);
        } else {
            outputStream.close();
            latch.await();
            return model;
        }
    }
}
