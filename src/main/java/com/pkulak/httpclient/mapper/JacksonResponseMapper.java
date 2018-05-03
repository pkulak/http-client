package com.pkulak.httpclient.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pkulak.httpclient.HttpClient;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * An {@link AsyncHandler} that uses a byte buffer when the response is small (under 2K) and streaming when otherwise
 * or unknown (chunked). The downside to streaming is that it requires a separate thread, but it doesn't need to buffer
 * the entire body before it can be processed.
 *
 * @param <T> the model type to return from the {@link HttpClient}
 */
public class JacksonResponseMapper<T> implements AsyncHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(JacksonResponseMapper.class);

    private final ObjectMapper mapper;
    private final Class<T> modelType;
    private final ObjectPool<ByteArrayOutputStream> bytePool;
    private final int cutoff;

    private boolean streaming = false;
    private int totalBytes = 0;
    private int totalParts = 0;

    // use this if the content size is small
    private ByteArrayOutputStream byteOutputStream;

    // switch to these if the content becomes large (more than one part)
    private PipedInputStream inputStream;
    private PipedOutputStream pipedOutputStream;
    private T model;
    private CountDownLatch latch;
    private final ExecutorService executor;

    // if there's an error, don't worry about trying to parse, since it could be anything
    private VoidResponseMapper voidResponseMapper;

    private BodyListener bodyListener;

    public JacksonResponseMapper(JacksonResponseMapperConfig<T> config) {
        this.mapper = config.getMapper();
        this.modelType = config.getModelType();
        this.executor = config.getExecutor();
        this.bytePool = config.getBytePool();
        this.cutoff = config.getCutoff();
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
        pipedOutputStream = new PipedOutputStream(inputStream);

        executor.execute(() -> {
            try {
                model = mapper.readValue(inputStream, modelType);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                latch.countDown();
            }
        });

        if (byteOutputStream != null) {
            pipedOutputStream.write(byteOutputStream.toByteArray());
            returnBufferToPool();
        }

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

        if (!streaming && totalBytes >= cutoff) {
            switchToStreaming();
        }

        if (streaming) {
            pipedOutputStream.write(bodyPart.getBodyPartBytes());
        } else {
            if (byteOutputStream == null) {
                byteOutputStream = bytePool.borrowObject();
            }

            byteOutputStream.write(bodyPart.getBodyPartBytes());
        }

        return State.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) {
        returnBufferToPool();
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
            log.debug("streamed response: {}", getStats());

            pipedOutputStream.close();
            latch.await();
            return model;
        } else {
            try {
                log.debug("buffered response: {}", getStats());

                if (totalBytes == 0) return null;

                return mapper.readValue(byteOutputStream.toByteArray(), modelType);
            } finally {
                returnBufferToPool();
            }
        }
    }

    private String getStats() {
        return String.format("%d bytes, %d parts, %d pooled",
                totalBytes, totalParts, bytePool.getNumActive() + bytePool.getNumIdle());
    }

    private void returnBufferToPool() {
        if (byteOutputStream != null) {
            try {
                bytePool.returnObject(byteOutputStream);
            } catch (Exception e) {
                log.error("could not return buffer to pool", e);
            }

            byteOutputStream = null;
        }
    }

    public interface BodyListener {
        void onBodyPartReceived(HttpResponseBodyPart bodyPart);

        void onRequestComplete();
    }

    public interface JacksonResponseMapperConfig<T> {
        // the mapper used to translate response bodies into model objects
        ObjectMapper getMapper();

        // the type to translate to
        Class<T> getModelType();

        // the executor used if the response is large enough to require streaming
        ExecutorService getExecutor();

        // the byte pool used for responses small enough to use buffering
        ObjectPool<ByteArrayOutputStream> getBytePool();

        // the body size cutoff (in bytes) for when a buffered response converts to streaming
        int getCutoff();
    }

    public static class ByteArrayOutputStreamFactory extends BasePooledObjectFactory<ByteArrayOutputStream> {
        @Override
        public ByteArrayOutputStream create() {
            return new ByteArrayOutputStream();
        }

        @Override
        public PooledObject<ByteArrayOutputStream> wrap(ByteArrayOutputStream out) {
            return new DefaultPooledObject<>(out);
        }

        @Override
        public void passivateObject(PooledObject<ByteArrayOutputStream> pooledObject) {
            pooledObject.getObject().reset();
        }
    }
}

