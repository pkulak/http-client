package com.pkulak.httpclient.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
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
public class JacksonResponseMapper<T> implements AsyncHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(JacksonResponseMapper.class);
    private static final int STREAMING_CUTOFF = 1024;

    private final ObjectMapper mapper;
    private final Class<T> modelType;

    private boolean streaming = false;
    private int totalBytes = 0;
    private int totalParts = 0;

    // use this if the content size is small
    private ByteArrayDataOutput buffer = ByteStreams.newDataOutput();

    // switch to these if the content becomes large (more than one part)
    private PipedInputStream inputStream;
    private PipedOutputStream outputStream;
    private T model;
    private CountDownLatch latch;
    private final ExecutorService executor;

    // if there's an error, don't worry about trying to parse, since it could be anything
    private VoidResponseMapper voidResponseMapper;

    private BodyListener bodyListener;

    public JacksonResponseMapper(ObjectMapper mapper, Class<T> modelType, ExecutorService executor) {
        this.mapper = mapper;
        this.modelType = modelType;
        this.executor = executor;
    }

    public static <U> Supplier<JacksonResponseMapper<U>> supplier(
            ObjectMapper mapper, Class<U> modelType, ExecutorService executor) {
        return () -> new JacksonResponseMapper<>(mapper, modelType, executor);
    }

    public void setBodyListener(BodyListener bodyListener) {
        this.bodyListener = bodyListener;
    }

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) {
        if (VoidResponseMapper.isErrorResponseCode(responseStatus)) {
            voidResponseMapper = new VoidResponseMapper();
            voidResponseMapper.onStatusReceived(responseStatus);
        }

        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(HttpHeaders headers) throws Exception {
        if (voidResponseMapper != null) {
            voidResponseMapper.onHeadersReceived(headers);
            return State.CONTINUE;
        }

        return State.CONTINUE;
    }

    private void switchToStreaming() throws IOException {
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

        outputStream.write(buffer.toByteArray());
        buffer = null;
        streaming = true;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
        if (bodyListener != null) {
            bodyListener.onBodyPartReceived(bodyPart);
        }

        if (voidResponseMapper != null) {
            voidResponseMapper.onBodyPartReceived(bodyPart);
            return State.CONTINUE;
        }

        totalBytes += bodyPart.length();
        totalParts += 1;

        if (!streaming && totalBytes >= STREAMING_CUTOFF) {
            switchToStreaming();
        }

        if (streaming) {
            outputStream.write(bodyPart.getBodyPartBytes());
        } else {
            buffer.write(bodyPart.getBodyPartBytes());
        }

        return State.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) {
        if (bodyListener != null) {
            bodyListener.onRequestComplete();
        }
    }

    @Override
    public T onCompleted() throws Exception {
        if (bodyListener != null) {
            bodyListener.onRequestComplete();
        }

        if (voidResponseMapper != null) {
            voidResponseMapper.onCompleted();
            throw new IllegalStateException("this should never happen");
        }

        if (streaming) {
            log.debug("streamed response: {} bytes, {} parts", totalBytes, totalParts);

            outputStream.close();
            latch.await();
            return model;
        } else {
            log.debug("buffered response: {} bytes, {} parts", totalBytes, totalParts);

            if (totalBytes == 0) return null;

            return mapper.readValue(buffer.toByteArray(), modelType);
        }
    }

    public interface BodyListener {
        void onBodyPartReceived(HttpResponseBodyPart bodyPart);

        void onRequestComplete();
    }
}
