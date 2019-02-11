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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

final class CooperativeWorker implements Runnable {
    private static final int COOPERATIVE_LOGGING_THRESHOLD = 5;
    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));

    @Probe(name = "taskletCount")
    private final List<TaskletTracker> trackers;
    @Probe
    private final AtomicLong iterationCount = new AtomicLong();
    private final ILogger logger;
    private final AtomicReference<Boolean> gracefulShutdown;
    private boolean madeProgress;

    CooperativeWorker(AtomicReference<Boolean> gracefulShutdown, LoggingService loggingService) {
        this.trackers = new CopyOnWriteArrayList<>();
        this.logger = loggingService.getLogger(CooperativeWorker.class);
        this.gracefulShutdown = gracefulShutdown;
    }

    @Override
    public void run() {
        long idleCount = 0;
        TrackerConsumer trackerConsumer = new TrackerConsumer();
        while (true) {
            Boolean gracefulShutdownLocal = gracefulShutdown.get();
            // exit condition
            if (gracefulShutdownLocal != null && (!gracefulShutdownLocal || trackers.isEmpty())) {
                break;
            }
            madeProgress = false;
            trackers.forEach(trackerConsumer);
            lazyIncrement(iterationCount);
            if (madeProgress) {
                idleCount = 0;
            } else {
                IDLER.idle(++idleCount);
            }
        }
        // Best-effort attempt to release all tasklets. A tasklet can still be added
        // to a dead worker through work stealing.
        trackers.forEach(t -> t.getExecutionTracker().taskletDone());
        trackers.clear();
    }

    void addTrackers(List<TaskletTracker> toBeAdded) {
        trackers.addAll(toBeAdded);
    }

    private void dismissTasklet(TaskletTracker t) {
        logFinest(logger, "Tasklet %s is done", t.getTasklet());
        t.getExecutionTracker().taskletDone();
        trackers.remove(t);
    }

    private final class TrackerConsumer implements Consumer<TaskletTracker> {
        private final Thread currentThread;

        private TrackerConsumer() {
            this.currentThread = Thread.currentThread();
        }

        @Override
        public void accept(TaskletTracker t) {
            long start = 0;
            if (logger.isFinestEnabled()) {
                start = System.nanoTime();
            }
            try {
                currentThread.setContextClassLoader(t.getJobClassLoader());
                final ProgressState result = t.getTasklet().call();
                if (result.isDone()) {
                    dismissTasklet(t);
                } else {
                    madeProgress |= result.isMadeProgress();
                }
            } catch (Throwable e) {
                logger.warning("Exception in " + t.getTasklet(), e);
                t.getExecutionTracker().exception(new JetException("Exception in " + t.getTasklet() + ": " + e, e));
            }
            if (t.getExecutionTracker().executionCompletedExceptionally()) {
                dismissTasklet(t);
            }

            if (logger.isFinestEnabled()) {
                long elapsedMs = NANOSECONDS.toMillis((System.nanoTime() - start));
                if (elapsedMs > COOPERATIVE_LOGGING_THRESHOLD) {
                    logger.finest("Cooperative tasklet call of '" + t.getTasklet() + "' took more than "
                            + COOPERATIVE_LOGGING_THRESHOLD + " ms: " + elapsedMs + "ms");
                }
            }
        }
    }
}
