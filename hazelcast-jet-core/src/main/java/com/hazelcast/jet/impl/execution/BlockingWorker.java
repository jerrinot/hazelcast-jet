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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.regex.Matcher.quoteReplacement;

final class BlockingWorker implements Runnable {
    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(5));

    private final TaskletTracker tracker;
    private final CountDownLatch startedLatch;
    private final AtomicInteger blockingWorkerCount;
    private final ILogger logger;
    private final AtomicReference<Boolean> gracefulShutdown;

    BlockingWorker(TaskletTracker tracker, CountDownLatch startedLatch,
                   AtomicInteger blockingWorkerCount, AtomicReference<Boolean> gracefulShutdown,
                   LoggingService loggingService) {
        this.tracker = tracker;
        this.startedLatch = startedLatch;
        this.blockingWorkerCount = blockingWorkerCount;
        this.logger = loggingService.getLogger(BlockingWorker.class);
        this.gracefulShutdown = gracefulShutdown;
    }

    @Override
    public void run() {
        final ClassLoader clBackup = currentThread().getContextClassLoader();
        final Tasklet t = tracker.getTasklet();
        final String oldName = currentThread().getName();
        currentThread().setContextClassLoader(tracker.getJobClassLoader());

        // swap the thread name by replacing the ".thread-NN" part at the end
        try {
            currentThread().setName(oldName.replaceAll(".thread-[0-9]+$", quoteReplacement("." + tracker.getTasklet())));
            assert !oldName.equals(currentThread().getName()) : "unexpected thread name pattern: " + oldName;
            blockingWorkerCount.incrementAndGet();

            startedLatch.countDown();
            t.init();
            long idleCount = 0;
            ProgressState result;
            do {
                result = t.call();
                if (result.isMadeProgress()) {
                    idleCount = 0;
                } else {
                    IDLER.idle(++idleCount);
                }
            } while (!result.isDone()
                    && !tracker.getExecutionTracker().executionCompletedExceptionally()
                    && !Boolean.FALSE.equals(gracefulShutdown.get()));
        } catch (Throwable e) {
            logger.warning("Exception in " + t, e);
            tracker.getExecutionTracker().exception(new JetException("Exception in " + t + ": " + e, e));
        } finally {
            blockingWorkerCount.decrementAndGet();
            currentThread().setContextClassLoader(clBackup);
            currentThread().setName(oldName);
            tracker.getExecutionTracker().taskletDone();
        }
    }
}
