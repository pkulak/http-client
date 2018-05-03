package com.pkulak.httpclient;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

public class RequestExecutor {
    private final LinkedBlockingDeque<RequestExecutor.RequestTask> deque;
    private final Semaphore semaphore;
    private final int permits;

    public RequestExecutor(int permits) {
        this(new Semaphore(permits), permits);
    }

    private RequestExecutor(Semaphore semaphore, int permits) {
        this.deque = new LinkedBlockingDeque<>();
        this.semaphore = semaphore;
        this.permits = permits;
    }

    public <T> CompletableFuture<T> execute(
            Supplier<? extends AsyncHandler<T>> responseHandler,
            BoundRequestBuilder requestBuilder) {
        RequestTask<T> task = new RequestTask<T>(requestBuilder, responseHandler);
        deque.add(task);
        checkQueue();
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

    @SuppressWarnings("unchecked")
    private void checkQueue() {
        if (semaphore.tryAcquire()) {
            RequestTask task = deque.pollFirst();

            if (task == null) {
                semaphore.release();
                return;
            }

            task.requestBuilder
                    .execute((AsyncHandler) task.responseHandler.get())
                    .toCompletableFuture()
                    .whenComplete((model, throwable) -> {
                        semaphore.release();
                        checkQueue();

                        if (throwable != null) {
                            task.completableFuture.completeExceptionally((Throwable) throwable);
                        } else {
                            task.completableFuture.complete(model);
                        }
                    });
        }
    }

    private static class RequestTask<T> {
        final BoundRequestBuilder requestBuilder;
        final CompletableFuture<T> completableFuture;
        final Supplier<? extends AsyncHandler<T>> responseHandler;

        public RequestTask(BoundRequestBuilder requestBuilder, Supplier<? extends AsyncHandler<T>> responseHandler) {
            this.requestBuilder = requestBuilder;
            this.responseHandler = responseHandler;
            this.completableFuture = new CompletableFuture<>();
        }
    }
}
