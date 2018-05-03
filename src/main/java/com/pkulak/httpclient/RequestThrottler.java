package com.pkulak.httpclient;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

public class RequestThrottler<T> {
    // we keep these guys in a tree structure
    private final RequestThrottler<?> root;
    private final Set<RequestThrottler<?>> children;

    private final Supplier<? extends AsyncHandler<T>> responseHandler;
    private final LinkedBlockingDeque<RequestTask<T>> deque;
    private final Semaphore semaphore;
    private final int permits;

    public RequestThrottler(Supplier<? extends AsyncHandler<T>> responseHandler, int maxConcurrency) {
        this(null, responseHandler, new Semaphore(maxConcurrency), maxConcurrency);
    }

    public <U> RequestThrottler<U> withHandler(Supplier<? extends AsyncHandler<U>> newHandler) {
        RequestThrottler<U> throttler = new RequestThrottler<>(root, newHandler, semaphore, permits);
        this.children.add(throttler);
        return throttler;
    }

    public RequestThrottler<T> withMaxConcurrency(int maxConcurrency) {
        return new RequestThrottler<>(null, responseHandler, new Semaphore(maxConcurrency), maxConcurrency);
    }

    private RequestThrottler(
            RequestThrottler<?> root,
            Supplier<? extends AsyncHandler<T>> responseHandler,
            Semaphore semaphore,
            int permits) {
        this.root = root == null ? this : root;
        this.children = Collections.newSetFromMap(new WeakHashMap<>());
        this.responseHandler = responseHandler;
        this.deque = new LinkedBlockingDeque<>();
        this.semaphore = semaphore;
        this.permits = permits;
    }

    public CompletableFuture<T> execute(BoundRequestBuilder requestBuilder) {
        if (semaphore.tryAcquire()) {
            // no need to queue anything, just go for it
            return requestBuilder
                    .execute(responseHandler.get())
                    .toCompletableFuture()
                    .whenComplete((model, throwable) -> {
                        semaphore.release();
                        checkQueue();
                    });
        }

        RequestTask<T> task = new RequestTask<>(requestBuilder);
        deque.add(task);
        return task.completableFuture;
    }

    /**
     * Blocks the current thread until a request is available to be run immediately.
     */
    public void await() throws InterruptedException {
        semaphore.acquire();
        semaphore.release();
    }

    /**
     * Blocks the current thread until all requests have completed.
     */
    public void awaitAll() throws InterruptedException {
        while (semaphore.availablePermits() < permits) {
            Thread.sleep(100);
        }
    }

    private void checkQueue() {
        root.checkQueueTree();
    }

    private void checkQueueTree() {
        // depth-first, all the way down the tree
        children.forEach(RequestThrottler::checkQueueTree);

        if (semaphore.tryAcquire()) {
            RequestTask<T> task = deque.pollFirst();

            if (task == null) {
                semaphore.release();
                return;
            }

            task.requestBuilder
                    .execute(responseHandler.get())
                    .toCompletableFuture()
                    .whenComplete((model, throwable) -> {
                        semaphore.release();
                        checkQueue();

                        if (throwable != null) {
                            task.completableFuture.completeExceptionally(throwable);
                        } else {
                            task.completableFuture.complete(model);
                        }
                    });
        }
    }

    private static class RequestTask<T> {
        final BoundRequestBuilder requestBuilder;
        final CompletableFuture<T> completableFuture;

        public RequestTask(BoundRequestBuilder requestBuilder) {
            this.requestBuilder = requestBuilder;
            this.completableFuture = new CompletableFuture<>();
        }
    }
}
