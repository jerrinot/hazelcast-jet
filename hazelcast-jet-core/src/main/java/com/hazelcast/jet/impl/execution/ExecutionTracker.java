/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static java.util.Collections.emptyList;

/**
 * Internal utility class to track the overall state of tasklet execution.
 * There's one instance of this class per job.
 */
final class ExecutionTracker {

    private final NonCompletableFuture future = new NonCompletableFuture();
    private volatile List<Future> blockingFutures = emptyList();

    private final AtomicInteger completionLatch;
    private final AtomicReference<Throwable> executionException = new AtomicReference<>();

    ExecutionTracker(int taskletCount, CompletableFuture<Void> cancellationFuture, LoggingService loggingService) {
        this.completionLatch = new AtomicInteger(taskletCount);
        ILogger logger = loggingService.getLogger(ExecutionTracker.class);
        cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
            if (e == null) {
                e = new IllegalStateException("cancellationFuture should be completed exceptionally");
            }
            exception(e);
            blockingFutures.forEach(f -> f.cancel(true)); // CompletableFuture.cancel ignores the flag
        }));
    }

    void exception(Throwable t) {
        executionException.compareAndSet(null, t);
    }

    void taskletDone() {
        if (completionLatch.decrementAndGet() == 0) {
            Throwable ex = executionException.get();
            if (ex == null) {
                future.internalComplete();
            } else {
                future.internalCompleteExceptionally(ex);
            }
        }
    }

    boolean executionCompletedExceptionally() {
        return executionException.get() != null;
    }

    public NonCompletableFuture getFuture() {
        return future;
    }

    public void setBlockingFutures(List<Future> blockingFutures) {
        this.blockingFutures = blockingFutures;
    }
}
