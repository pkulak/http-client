package com.pkulak.httpclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pkulak.httpclient.mapper.FormRequestMapper;
import com.pkulak.httpclient.mapper.JacksonRequestMapper;
import com.pkulak.httpclient.mapper.JacksonResponseMapper;
import com.pkulak.httpclient.mapper.RequestMapper;
import com.pkulak.httpclient.mapper.StatusResponseMapper;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * A wrapper around the amazing {@link AsyncHttpClient} that uses the "mutant factory" pattern. This means that
 * instances are immutable, and can be shared with abandon, and all modification methods return new, still immutable
 * instances.
 *
 * @param <T> the model entity type returned from responses
 * @param <I> the model entity type to be converted into requests
 */
public class HttpClient<T, I> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

    private final AsyncHttpClient asycHttpClient;
    private final ObjectMapper mapper;
    private final ExecutorService executor;
    private final RequestThrottler<T> requestThrottler;
    private final RequestMapper<I> requestHandler;
    private final Request request;

    /**
     * Create a new {@link HttpClient}. Before it can be used, the url, at the very least, must be set.
     *
     * @return a new {@link HttpClient}
     */
    public static HttpClient<JsonNode, Object> createDefault() {
        return createDefault(new DefaultAsyncHttpClient(), new ObjectMapper(), null);
    }

    /**
     * Create a new {@link HttpClient} by specifying only the url. An {@link ObjectMapper} and {@link AsyncHttpClient}
     * will be created with the default constructors.
     *
     * @param url the url that will initially be set
     * @return a new {@link HttpClient}
     */
    public static HttpClient<JsonNode, Object> createDefault(String url) {
        return createDefault(new DefaultAsyncHttpClient(), new ObjectMapper(), url);
    }

    /**
     * Create a new {@link HttpClient} by specifying the url, {@link AsyncHttpClient} and {@link ObjectMapper}.
     *
     * @param asycHttpClient the async HTTP client to use for all requests
     * @param mapper the object mapper to use for mapping requests and responses
     * @param url the url that will be initially set
     * @return a new {@link HttpClient}
     */
    public static HttpClient<JsonNode, Object> createDefault(
            AsyncHttpClient asycHttpClient,
            ObjectMapper mapper,
            String url) {

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("http-client-%d")
                .build());

        return new HttpClient<>(
                asycHttpClient,
                mapper,
                executor,
                new RequestThrottler<>(JacksonResponseMapper.supplier(mapper, JsonNode.class, executor), 4),
                new JacksonRequestMapper(mapper),
                Request.builder(url).build());
    }

    private HttpClient(
            AsyncHttpClient asycHttpClient,
            ObjectMapper mapper,
            ExecutorService executor,
            RequestThrottler<T> requestThrottler,
            RequestMapper<I> requestHandler,
            Request request) {
        this.asycHttpClient = asycHttpClient;
        this.mapper = mapper;
        this.executor = executor;
        this.requestThrottler = requestThrottler;
        this.requestHandler = requestHandler;
        this.request = request;
    }

    private HttpClient<T, I> clone(Request request) {
        return new HttpClient<T, I>(
                asycHttpClient, mapper, executor, requestThrottler, requestHandler, request);
    }

    @Override
    public void close() throws Exception {
        asycHttpClient.close();
    }

    @Override
    public String toString() {
        return request.toString();
    }

    /**
     * Returns a new client that will return an integer response status only. Responses are closed immediately after
     * the status line is received.
     */
    public HttpClient<Integer, I> statusOnly() {
        return responseMapper(StatusResponseMapper.supplier());
    }

    /**
     * Returns a new client that will use Jackson to map to the given model type.
     *
     * @param modelType the class of the type to map to
     * @param <U> the type to map to
     */
    public <U> HttpClient<U, I> forModelType(Class<U> modelType) {
        return responseMapper(JacksonResponseMapper.supplier(mapper, modelType, executor));
    }

    /**
     * Returns a new client that will use the given response mapper.
     *
     * @param newMapper the new mapper to use
     * @param <U> the type to be returned from future responses
     */
    public <U> HttpClient<U, I> responseMapper(Supplier<? extends AsyncHandler<U>> newMapper) {
        return new HttpClient<U, I>(
                asycHttpClient, mapper, executor,
                requestThrottler.withHandler(newMapper),
                requestHandler, request);
    }

    /**
     * Returns a new client that will encode requests using form encoding.
     */
    public HttpClient<T, Multimap<String, Object>> withForm() {
        return requestMapper(FormRequestMapper.INSTANCE);
    }

    /**
     * Returns a new client that will use the given request mapper.
     *
     * @param newMapper the new mapper to use
     * @param <U> the type to be converted to bytes for the request body
     */
    public <U> HttpClient<T, U> requestMapper(RequestMapper<U> newMapper) {
        return new HttpClient<T, U>(
                asycHttpClient, mapper, executor, requestThrottler, newMapper, request);
    }

    /**
     * Returns a new client with the given max concurrency. The client returned, and all clients created from it,
     * will share in a request pool the size of the concurrency passed in here. Every time this is called, and brand
     * new pool is created.
     *
     * @param max the maximum number of outstanding requests allowed for this client and its children
     */
    public HttpClient<T, I> maxConcurrency(int max) {
        return new HttpClient<T, I>(
                asycHttpClient, mapper, executor,
                requestThrottler.withMaxConcurrency(max),
                requestHandler, request);
    }

    /**
     * Returns a new client with the given HTTP request method.
     *
     * @param method the method to use
     */
    public HttpClient<T, I> method(String method) {
        return clone(request.toBuilder().method(method).build());
    }

    /**
     * Returns a new client with the given URL. This will also set the path (to nothing if there's no path in the URL),
     * and query parameters (also to nothing if they are not present in the URL).
     *
     * @param url the url to use
     */
    public HttpClient<T, I> url(String url) {
        return clone(request.toBuilder().url(url).build());
    }

    /**
     * Returns a new client with the given path.
     *
     * @param path the path to use
     */
    public HttpClient<T, I> setPath(String path) {
        return clone(request.toBuilder().path(path).build());
    }


    /**
     * Returns a new client with the given value appended to the current path.
     *
     * @param path the new path segment to append
     */
    public HttpClient<T, I> addPath(String path) {
        return clone(request.toBuilder().addPath(path).build());
    }

    /**
     * Returns a new client with the given path parameter set. Parameters are specified in the path enclosed by
     * curly brackets, "{" and "}".
     *
     * @param key the replacement key
     * @param value the replacement value
     * @return
     */
    public HttpClient<T, I> pathParam(String key, Object value) {
        return clone(request.toBuilder().pathParam(key, value).build());
    }

    /**
     * Returns a new client with the query parameter set. If multiple values are already set for this key, they will
     * be reset to only this value.
     *
     * @param key the parameter key
     * @param val the parameter value
     */
    public HttpClient<T, I> setQueryParam(String key, Object val) {
        return clone(request.toBuilder().setQueryParam(key, val).build());
    }

    /**
     * Returns a new client with the query parameter added. If one or more values are already set for this key, this
     * new value will be added to the existing values.
     *
     * @param key the parameter key
     * @param val the parameter value
     */
    public HttpClient<T, I> addQueryParam(String key, Object val) {
        return clone(request.toBuilder().addQueryParam(key, val).build());
    }

    /**
     * Returns a new client with the header set. If multiple values are already set for this key, they will be reset to
     * only this value.
     *
     * @param key the header key
     * @param val the header value
     */
    public HttpClient<T, I> setHeader(String key, String val) {
        return clone(request.toBuilder().setHeader(key, val).build());
    }

    /**
     * Returns a new client with the header added. If one or more values are already set for this key, this new value
     * will be added to the existing values.
     *
     * @param key the header key
     * @param val the header value
     */
    public HttpClient<T, I> addHeader(String key, String val) {
        return clone(request.toBuilder().addHeader(key, val).build());
    }

    /**
     * Set the method to "GET" and execute the request.
     *
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> getAsync() {
        return method("GET").executeAsync();
    }

    /**
     * Set the method to "GET" and execute the request.
     *
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T get() {
        return synchronize(getAsync());
    }

    /**
     * Set the method to "DELETE" and execute the request.
     *
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> deleteAsync() {
        return method("DELETE").executeAsync();
    }

    /**
     * Set the method to "DELETE" and execute the request.
     *
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T delete() {
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
    public CompletableFuture<T> putAsync(String contentType, I requestBody) {
        return method("PUT").executeAsync(contentType, requestBody);
    }

    /**
     * Set the method to "PUT" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> putAsync(I requestBody) {
        return putAsync(null, requestBody);
    }

    /**
     * Set the method to "PUT" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T put(I requestBody) {
        return synchronize(putAsync(requestBody));
    }

    /**
     * Set the method to "PUT" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T put(String contentType, I requestBody) {
        return synchronize(putAsync(contentType, requestBody));
    }

    /**
     * Set the method to "POST" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> postAsync(String contentType, I requestBody) {
        return method("POST").executeAsync(contentType, requestBody);
    }

    /**
     * Set the method to "POST" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> postAsync(I requestBody) {
        return postAsync(null, requestBody);
    }

    /**
     * Set the method to "POST" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T post(I requestBody) {
        return synchronize(postAsync(requestBody));
    }

    /**
     * Set the method to "POST" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T post(String contentType, I requestBody) {
        return synchronize(postAsync(contentType, requestBody));
    }

    /**
     * Set the method to "PATCH" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> patchAsync(String contentType, I requestBody) {
        return method("PATCH").executeAsync(contentType, requestBody);
    }

    /**
     * Set the method to "PATCH" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> patchAsync(I requestBody) {
        return patchAsync(null, requestBody);
    }

    /**
     * Set the method to "PATCH" and execute the request. The content type will be taken either from the existing
     * headers or the default for this type.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T patch(I requestBody) {
        return synchronize(patchAsync(requestBody));
    }

    /**
     * Set the method to "PATCH" and execute the request.
     *
     * @param contentType the MIME type of the POST body
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T patch(String contentType, I requestBody) {
        return synchronize(patchAsync(contentType, requestBody));
    }

    private <U> U synchronize(CompletableFuture<U> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new HttpException("Could not perform " + toString(), e);
        }
    }

    private InputStream mapRequest(I requestBody) {
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
    public CompletableFuture<T> executeAsync() {
        if (!request.isUrlSet()) {
            throw new HttpException("url has not been set");
        }

        return requestThrottler.execute(asycHttpClient
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
    public T execute() {
        return synchronize(executeAsync());
    }

    /**
     * Execute the request as it's currently built.
     *
     * @param requestBody the request body
     * @return a {@link CompletableFuture} that will complete with the request
     */
    public CompletableFuture<T> executeAsync(I requestBody) {
        return executeAsync(null, requestBody);
    }

    /**
     * Execute the request as it's currently built.
     *
     * @param requestBody the request body
     * @return the response value, after blocking the current thread until the request is complete
     */
    public T execute(I requestBody) {
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
    public CompletableFuture<T> executeAsync(String contentType, I requestBody) {
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

        return requestThrottler.execute(asycHttpClient
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
    public T execute(String contentType, I requestBody) {
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
}

