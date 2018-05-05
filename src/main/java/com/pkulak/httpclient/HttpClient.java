package com.pkulak.httpclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pkulak.httpclient.logging.LoggingRequestFilter;
import com.pkulak.httpclient.logging.LoggingResponseFilter;
import com.pkulak.httpclient.mapper.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A wrapper around the amazing {@link AsyncHttpClient} that uses the "mutant factory" pattern. This means that
 * instances are immutable, and can be shared with abandon, and all modification methods return new, still immutable
 * instances.
 *
 * @param <T> the type to be converted into request bodies
 * @param <R> the type to be created from response bodies
 */
public class HttpClient<T, R> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

    private final AsyncHttpClient asycHttpClient;
    private final JacksonConfig<R> jacksonConfig;
    private final RequestExecutor requestExecutor;
    private final Supplier<? extends AsyncHandler<R>> responseHandler;
    private final RequestMapper<T> requestHandler;
    private final Request request;

    /**
     * Create a new {@link HttpClient}. Before it can be used, the url, at the very least, must be set.
     *
     * @return a new {@link HttpClient}
     */
    public static HttpClient<Object, JsonNode> createDefault() {
        return createDefault(createAsyncHttpClient(), new ObjectMapper(), null);
    }

    /**
     * Create a new {@link HttpClient} by specifying only the url. An {@link ObjectMapper} and {@link AsyncHttpClient}
     * will be created with the default constructors.
     *
     * @param url the url that will initially be set
     * @return a new {@link HttpClient}
     */
    public static HttpClient<Object, JsonNode> createDefault(String url) {
        return createDefault(createAsyncHttpClient(), new ObjectMapper(), url);
    }

    private static AsyncHttpClient createAsyncHttpClient() {
        return new DefaultAsyncHttpClient(new DefaultAsyncHttpClientConfig.Builder()
                .addRequestFilter(new LoggingRequestFilter())
                .addResponseFilter(new LoggingResponseFilter())
                .build());
    }


    /**
     * Create a new {@link HttpClient} by specifying the url, {@link AsyncHttpClient} and {@link ObjectMapper}.
     *
     * @param asycHttpClient the async HTTP client to use for all requests
     * @param mapper         the object mapper to use for mapping requests and responses
     * @param url            the url that will be initially set
     * @return a new {@link HttpClient}
     */
    public static HttpClient<Object, JsonNode> createDefault(
            AsyncHttpClient asycHttpClient,
            ObjectMapper mapper,
            String url) {

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("http-client-%d")
                .build());

        ObjectPool<ByteArrayOutputStream> bytePool =
                new GenericObjectPool<>(new JacksonResponseMapper.ByteArrayOutputStreamFactory());

        JacksonConfig<JsonNode> jacksonConfig = new JacksonConfig<>(mapper, JsonNode.class, executor, bytePool, 1024);

        return new HttpClient<>(
                asycHttpClient,
                jacksonConfig,
                new RequestExecutor(4),
                () -> new JacksonResponseMapper<>(jacksonConfig),
                new JacksonRequestMapper(mapper),
                Request.builder(url).build());
    }

    private HttpClient(
            AsyncHttpClient asycHttpClient,
            JacksonConfig<R> jacksonConfig,
            RequestExecutor requestExecutor,
            Supplier<? extends AsyncHandler<R>> responseHandler,
            RequestMapper<T> requestHandler,
            Request request) {
        this.asycHttpClient = asycHttpClient;
        this.jacksonConfig = jacksonConfig;
        this.requestExecutor = requestExecutor;
        this.responseHandler = responseHandler;
        this.requestHandler = requestHandler;
        this.request = request;
    }

    private HttpClient<T, R> clone(Request request) {
        return new HttpClient<T, R>(
                asycHttpClient, jacksonConfig, requestExecutor, responseHandler, requestHandler, request);
    }

    public ObjectMapper getMapper() {
        return jacksonConfig.getMapper();
    }

    /**
     * @see RequestExecutor#await() await
     */
    public void await() throws InterruptedException {
        requestExecutor.await();
    }

    /**
     * @see RequestExecutor#awaitAll() awaitAll
     */
    public void awaitAll() throws InterruptedException {
        requestExecutor.awaitAll();
    }

    @Override
    public void close() throws Exception {
        asycHttpClient.close();
        jacksonConfig.shutdown();
    }

    @Override
    public String toString() {
        return request.toString();
    }

    /**
     * Returns a new client that will return an integer response status only. Responses are closed immediately after
     * the status line is received.
     */
    public HttpClient<T, Integer> statusOnly() {
        return responseMapper(StatusResponseMapper::new);
    }

    /**
     * Returns a new client that will return the entire response as a string with no other parsing.
     */
    public HttpClient<T, String> stringResponse() {
        return responseMapper(StringResponseMapper::new);
    }

    /**
     * Returns a new client that will return nothing. It will, however, throw if the response is not a success.
     */
    public HttpClient<T, Void> voidResponse() {
        return responseMapper(VoidResponseMapper::new);
    }

    /**
     * Returns a new client that will use Jackson to map to the given model type.
     *
     * @param modelType the class of the type to map to
     * @param <U> the type to map to
     */
    public <U> HttpClient<T, U> forModelType(Class<U> modelType) {
        return responseMapper(() -> new JacksonResponseMapper<>(jacksonConfig.withModelType(modelType)));
    }

    /**
     * Returns a new client that will use the given object mapper for requests AND responses.
     *
     * @param mapper the new object mapper to use
     */
    @SuppressWarnings("unchecked")
    public HttpClient<T, R> objectMapper(ObjectMapper mapper) {
        JacksonConfig<R> newConfig = jacksonConfig.withMapper(mapper);

        return new HttpClient<T, R>(
                asycHttpClient, newConfig,
                requestExecutor,
                () -> new JacksonResponseMapper<>(newConfig),
                (RequestMapper<T>) new JacksonRequestMapper(mapper),
                request);
    }

    /**
     * Returns a new client that will use the given response mapper.
     *
     * @param newMapper the new mapper to use
     * @param <U>       the type to be returned from future responses
     */
    @SuppressWarnings("unchecked")
    public <U> HttpClient<T, U> responseMapper(Supplier<? extends AsyncHandler<U>> newMapper) {
        return new HttpClient<T, U>(
                asycHttpClient,
                (JacksonConfig<U>) jacksonConfig.withModelType(Void.class),
                requestExecutor,
                newMapper,
                requestHandler,
                request);
    }

    /**
     * Returns a new client that will encode requests using form encoding.
     */
    public HttpClient<Multimap<String, Object>, R> withForm() {
        return requestMapper(FormRequestMapper.INSTANCE);
    }

    /**
     * Returns a new client that will use the given request mapper.
     *
     * @param newMapper the new mapper to use
     * @param <U>       the type to be converted to bytes for the request body
     */
    public <U> HttpClient<U, R> requestMapper(RequestMapper<U> newMapper) {
        return new HttpClient<U, R>(
                asycHttpClient, jacksonConfig, requestExecutor, responseHandler, newMapper, request);
    }

    /**
     * Returns a new client with the given max concurrency. The client returned, and all clients created from it,
     * will share in a request pool the size of the concurrency passed in here. Every time this is called, and brand
     * new pool is created.
     *
     * @param max the maximum number of outstanding requests allowed for this client and its children
     */
    public HttpClient<T, R> maxConcurrency(int max) {
        return new HttpClient<T, R>(
                asycHttpClient, jacksonConfig,
                new RequestExecutor(max),
                responseHandler, requestHandler, request);
    }

    /**
     * Returns a new client with the given HTTP request method.
     *
     * @param method the method to use
     */
    public HttpClient<T, R> method(String method) {
        return clone(request.toBuilder().method(method).build());
    }

    /**
     * Returns a new client with the given URL. This will also set the path (to nothing if there's no path in the URL),
     * and query parameters (also to nothing if they are not present in the URL).
     *
     * @param url the url to use
     */
    public HttpClient<T, R> url(String url) {
        return clone(request.toBuilder().url(url).build());
    }

    /**
     * Returns a new client with the given path.
     *
     * @param path the path to use
     */
    public HttpClient<T, R> setPath(String path) {
        return clone(request.toBuilder().path(path).build());
    }


    /**
     * Returns a new client with the given value appended to the current path.
     *
     * @param path the new path segment to append
     */
    public HttpClient<T, R> appendPath(String path) {
        return clone(request.toBuilder().appendPath(path).build());
    }

    /**
     * Returns a new client with the given path parameter set. Parameters are specified in the path enclosed by
     * curly brackets, "{" and "}".
     *
     * @param key the replacement key
     * @param val the replacement value
     */
    public HttpClient<T, R> pathParam(String key, Object val) {
        checkNotNull(val, "path param must not be null");
        String closed = val.toString();
        return clone(request.toBuilder().pathParam(key, () -> closed).build());
    }

    public HttpClient<T, R> pathParam(String key, Supplier<String> val) {
        return clone(request.toBuilder().pathParam(key, val).build());
    }

    /**
     * Returns a new client with the query parameter set. If multiple values are already set for this key, they will
     * be reset to only this value.
     *
     * @param key the parameter key
     * @param val the parameter value
     */
    public HttpClient<T, R> setQueryParam(String key, Object val) {
        checkNotNull(val, "query param must not be null");
        String closed = val.toString();
        return clone(request.toBuilder().setQueryParam(key, () -> closed).build());
    }

    public HttpClient<T, R> setQueryParam(String key, Supplier<String> val) {
        return clone(request.toBuilder().setQueryParam(key, val).build());
    }

    /**
     * Returns a new client with the query parameter added. If one or more values are already set for this key, this
     * new value will be added to the existing values.
     *
     * @param key the parameter key
     * @param val the parameter value
     */
    public HttpClient<T, R> addQueryParam(String key, Object val) {
        String closed = val.toString();
        return clone(request.toBuilder().addQueryParam(key, () -> closed).build());
    }

    public HttpClient<T, R> addQueryParam(String key, Supplier<String> val) {
        return clone(request.toBuilder().addQueryParam(key, val).build());
    }

    /**
     * Returns a new client with the header set. If multiple values are already set for this key, they will be reset to
     * only this value.
     *
     * @param key the header key
     * @param val the header value
     */
    public HttpClient<T, R> setHeader(String key, Object val) {
        String closed = val.toString();
        return clone(request.toBuilder().setHeader(key, () -> closed).build());
    }

    public HttpClient<T, R> setHeader(String key, Supplier<String> val) {
        return clone(request.toBuilder().setHeader(key, val).build());
    }

    /**
     * Returns a new client with the header added. If one or more values are already set for this key, this new value
     * will be added to the existing values.
     *
     * @param key the header key
     * @param val the header value
     */
    public HttpClient<T, R> addHeader(String key, Object val) {
        String closed = val.toString();
        return clone(request.toBuilder().addHeader(key, () -> closed).build());
    }

    public HttpClient<T, R> addHeader(String key, Supplier<String> val) {
        return clone(request.toBuilder().addHeader(key, val).build());
    }

    /**
     * Set the method to "GET" and execute the request.
     *
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> getAsync() {
        return method("GET").executeAsync();
    }

    /**
     * Set the method to "GET" and execute the request.
     *
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R get() {
        return synchronize(getAsync());
    }

    /**
     * Set the method to "DELETE" and execute the request.
     *
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> deleteAsync() {
        return method("DELETE").executeAsync();
    }

    /**
     * Set the method to "DELETE" and execute the request.
     *
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R delete() {
        return synchronize(deleteAsync());
    }

    /**
     * Set the method to "HEAD" and execute the request.
     *
     * @return a {@link CompletableFuture} that will contain the status of the response
     */
    public CompletableFuture<Integer> headAsync() {
        return method("HEAD").statusOnly().executeAsync();
    }

    /**
     * Set the method to "HEAD" and execute the request.
     *
     * @return the response status, after blocking the current thread until the request is complete
     */
    public int head() {
        return synchronize(headAsync());
    }

    /**
     * Set the method to "PUT" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> putAsync(String contentType, T requestBody) {
        return method("PUT").executeAsync(contentType, requestBody);
    }

    /**
     * Set the method to "PUT" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> putAsync(T requestBody) {
        return putAsync(null, requestBody);
    }

    /**
     * Set the method to "PUT" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R put(T requestBody) {
        return synchronize(putAsync(requestBody));
    }

    /**
     * Set the method to "PUT" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R put(String contentType, T requestBody) {
        return synchronize(putAsync(contentType, requestBody));
    }

    /**
     * Set the method to "POST" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> postAsync(String contentType, T requestBody) {
        return method("POST").executeAsync(contentType, requestBody);
    }

    /**
     * Set the method to "POST" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> postAsync(T requestBody) {
        return postAsync(null, requestBody);
    }

    /**
     * Set the method to "POST" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R post(T requestBody) {
        return synchronize(postAsync(requestBody));
    }

    /**
     * Set the method to "POST" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R post(String contentType, T requestBody) {
        return synchronize(postAsync(contentType, requestBody));
    }

    /**
     * Set the method to "PATCH" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> patchAsync(String contentType, T requestBody) {
        return method("PATCH").executeAsync(contentType, requestBody);
    }

    /**
     * Set the method to "PATCH" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> patchAsync(T requestBody) {
        return patchAsync(null, requestBody);
    }

    /**
     * Set the method to "PATCH" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R patch(T requestBody) {
        return synchronize(patchAsync(requestBody));
    }

    /**
     * Set the method to "PATCH" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R patch(String contentType, T requestBody) {
        return synchronize(patchAsync(contentType, requestBody));
    }

    private <U> U synchronize(CompletableFuture<U> future) {
        try {
            return future.get();
        } catch (Exception e) {
            if (e instanceof ExecutionException && e.getCause() instanceof Exception) {
                e = (Exception) e.getCause();
            }

            throw new HttpException("Could not perform " + toString(), e);
        }
    }

    private InputStream mapRequest(T requestBody) {
        try {
            return requestHandler.map(requestBody);
        } catch (Exception e) {
            log.error("Could not map request body object.", e);
            return new ByteArrayInputStream(new byte[] {});
        }
    }

    /**
     * Execute the request as it's currently built.
     *
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> executeAsync() {
        if (!request.isUrlSet()) {
            throw new HttpException("url has not been set");
        }

        return requestExecutor.execute(responseHandler, asycHttpClient
                .prepareRequest(new RequestBuilder()
                        .setMethod(this.request.getMethod())
                        .setUrl(this.request.getUrl()))
                .setHeaders(this.request.getHeaders())
                .setQueryParams(this.request.getQueryParams()));
    }

    /**
     * Execute the request as it's currently built.
     *
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R execute() {
        return synchronize(executeAsync());
    }

    /**
     * Execute the request as it's currently built.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> executeAsync(T requestBody) {
        return executeAsync(null, requestBody);
    }

    /**
     * Execute the request as it's currently built.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R execute(T requestBody) {
        return synchronize(executeAsync(requestBody));
    }

    /**
     * Execute the request as it's currently built, with the given body. If the content type is null or empty, the
     * existing header or default for the mapper will be used instead.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<R> executeAsync(String contentType, T requestBody) {
        if (!request.isUrlSet()) {
            throw new HttpException("url has not been set");
        }

        if (Strings.isNullOrEmpty(contentType)) {
            if (this.request.getHeaders().containsKey("Content-Type")) {
                contentType = this.request.getHeaders().get("Content-Type").iterator().next();
            } else {
                contentType = requestHandler.defaultContentType();
            }
        }

        return requestExecutor.execute(responseHandler, asycHttpClient
                .prepareRequest(new RequestBuilder()
                        .setMethod(this.request.getMethod())
                        .setUrl(this.request.getUrl()))
                .setHeaders(this.request.getHeaders())
                .setHeader("Content-Type", contentType)
                .setBody(mapRequest(requestBody))
                .setQueryParams(this.request.getQueryParams()));
    }

    /**
     * Execute the request as it's currently built, with the given body and content type.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public R execute(String contentType, T requestBody) {
        return synchronize(executeAsync(contentType, requestBody));
    }

    /**
     * Return the underlying {@link AsyncHttpClient}.
     */
    public AsyncHttpClient unwrap() {
        return asycHttpClient;
    }

    public static class HttpException extends RuntimeException {
        public HttpException(String message) {
            super(message);
        }

        public HttpException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class JacksonConfig<T> implements JacksonResponseMapper.JacksonResponseMapperConfig<T> {
        private final ObjectMapper mapper;
        private final Class<T> modelType;
        private final ExecutorService executor;
        private final ObjectPool<ByteArrayOutputStream> bytePool;
        private final int cutoff;

        JacksonConfig(
                ObjectMapper mapper,
                Class<T> modelType,
                ExecutorService executor,
                ObjectPool<ByteArrayOutputStream> bytePool,
                int cutoff) {
            this.mapper = mapper;
            this.modelType = modelType;
            this.executor = executor;
            this.bytePool = bytePool;
            this.cutoff = cutoff;
        }

        @Override
        public ObjectMapper getMapper() {
            return mapper;
        }

        @Override
        public Class<T> getModelType() {
            return modelType;
        }

        @Override
        public ExecutorService getExecutor() {
            return executor;
        }

        @Override
        public ObjectPool<ByteArrayOutputStream> getBytePool() {
            return bytePool;
        }

        @Override
        public int getCutoff() {
            return cutoff;
        }

        public <U> JacksonConfig<U> withModelType(Class<U> modelType) {
            return new JacksonConfig<>(mapper, modelType, executor, bytePool, cutoff);
        }

        public JacksonConfig<T> withMapper(ObjectMapper mapper) {
            return new JacksonConfig<T>(mapper, modelType, executor, bytePool, cutoff);
        }

        public void shutdown() {
            executor.shutdown();
        }
    }
}

