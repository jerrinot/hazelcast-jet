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
import com.hazelcast.jet.impl.exception.ShutdownInProgressException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class TaskletExecutionService {
    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final CooperativeWorker[] cooperativeWorkers;
    private final Thread[] cooperativeThreadPool;
    private final String hzInstanceName;
    private final ILogger logger;
    private int cooperativeThreadIndex;
    @Probe
    private final AtomicInteger blockingWorkerCount = new AtomicInteger();
    private final LoggingService loggingService;

    // tri-state boolean:
    // - null: not shut down
    // - FALSE: shut down forcefully: break the execution loop, don't wait for tasklets to finish
    // - TRUE: shut down gracefully: run normally, don't accept more tasklets
    private final AtomicReference<Boolean> gracefulShutdown = new AtomicReference<>(null);
    private final Object lock = new Object();

    public TaskletExecutionService(NodeEngineImpl nodeEngine, int threadCount) {
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.cooperativeWorkers = new CooperativeWorker[threadCount];
        this.cooperativeThreadPool = new Thread[threadCount];
        this.loggingService = nodeEngine.getLoggingService();
        this.logger = loggingService.getLogger(TaskletExecutionService.class);

        nodeEngine.getMetricsRegistry().newProbeBuilder()
                       .withTag("module", "jet")
                       .scanAndRegister(this);

        Arrays.setAll(cooperativeWorkers, i -> new CooperativeWorker(gracefulShutdown, loggingService));
        Arrays.setAll(cooperativeThreadPool, i -> new Thread(cooperativeWorkers[i],
                String.format("hz.%s.jet.cooperative.thread-%d", hzInstanceName, i)));
        Arrays.stream(cooperativeThreadPool).forEach(Thread::start);
        for (int i = 0; i < cooperativeWorkers.length; i++) {
            nodeEngine.getMetricsRegistry().newProbeBuilder()
                           .withTag("module", "jet")
                           .withTag("cooperativeWorker", String.valueOf(i))
                           .scanAndRegister(cooperativeWorkers[i]);
        }
    }

    /**
     * Submits the tasklets for execution and returns a future which gets
     * completed when the execution of all the tasklets has completed. If an
     * exception occurrs or the execution gets cancelled, the future will be
     * completed exceptionally, but only after all the tasklets have finished
     * executing. The returned future does not support cancellation, instead
     * the supplied {@code cancellationFuture} should be used.
     *
     * @param tasklets            tasklets to run
     * @param cancellationFuture  future that, if cancelled, will cancel the execution of the tasklets
     * @param jobClassLoader      classloader to use when running the tasklets
     */
    CompletableFuture<Void> beginExecute(
            @Nonnull List<? extends Tasklet> tasklets,
            @Nonnull CompletableFuture<Void> cancellationFuture,
            @Nonnull ClassLoader jobClassLoader
    ) {
        if (gracefulShutdown.get() != null) {
            throw new ShutdownInProgressException();
        }
        final ExecutionTracker executionTracker = new ExecutionTracker(tasklets.size(), cancellationFuture,
                loggingService);
        try {
            final Map<Boolean, List<Tasklet>> byCooperation =
                    tasklets.stream().collect(partitioningBy(Tasklet::isCooperative));
            submitCooperativeTasklets(executionTracker, jobClassLoader, byCooperation.get(true));
            submitBlockingTasklets(executionTracker, jobClassLoader, byCooperation.get(false));
        } catch (Throwable t) {
            executionTracker.getFuture().internalCompleteExceptionally(t);
        }
        return executionTracker.getFuture();
    }

    public void shutdown(boolean graceful) {
        if (gracefulShutdown.compareAndSet(null, graceful)) {
            if (graceful) {
                blockingTaskletExecutor.shutdown();
            } else {
                blockingTaskletExecutor.shutdownNow();
            }
        }
    }

    private void submitBlockingTasklets(ExecutionTracker executionTracker, ClassLoader jobClassLoader,
                                        List<Tasklet> tasklets) {
        CountDownLatch startedLatch = new CountDownLatch(tasklets.size());
        executionTracker.setBlockingFutures(tasklets
                .stream()
                .map(t -> new BlockingWorker(new TaskletTracker(t, executionTracker, jobClassLoader), startedLatch,
                        blockingWorkerCount, gracefulShutdown, loggingService))
                .map(blockingTaskletExecutor::submit)
                .collect(toList()));

        // do not return from this method until all workers have started. Otherwise
        // on cancellation there is a race where the executor might not have started
        // the worker yet. This would results in taskletDone() never being called for
        // a worker.
        uncheckRun(startedLatch::await);
    }

    private void submitCooperativeTasklets(
            ExecutionTracker executionTracker, ClassLoader jobClassLoader, List<Tasklet> tasklets
    ) {
        @SuppressWarnings("unchecked")
        final List<TaskletTracker>[] trackersByThread = new List[cooperativeWorkers.length];
        Arrays.setAll(trackersByThread, i -> new ArrayList());
        for (Tasklet t : tasklets) {
            t.init();
        }

        // We synchronize so that no two jobs submit their tasklets in
        // parallel. If two jobs submit in parallel, the tasklets of one of
        // them could happen to not use all threads. When the other one ends,
        // some worker might have no tasklet.
        synchronized (lock) {
            for (Tasklet t : tasklets) {
                trackersByThread[cooperativeThreadIndex].add(new TaskletTracker(t, executionTracker, jobClassLoader));
                cooperativeThreadIndex = (cooperativeThreadIndex + 1) % trackersByThread.length;
            }
        }
        for (int i = 0; i < trackersByThread.length; i++) {
            cooperativeWorkers[i].addTrackers(trackersByThread[i]);
        }
        Arrays.stream(cooperativeThreadPool).forEach(LockSupport::unpark);
    }

    /**
     * Blocks until all workers terminate (cooperative & blocking).
     */
    public void awaitWorkerTermination() {
        assert gracefulShutdown.get() != null : "Not shut down";
        try {
            while (!blockingTaskletExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                logger.warning("Blocking tasklet executor did not terminate in 1 minute");
            }
            for (Thread t : cooperativeThreadPool) {
                t.join();
            }
        } catch (InterruptedException e) {
            sneakyThrow(e);
        }
    }

    private final class BlockingTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            return new Thread(r,
                    String.format("hz.%s.jet.blocking.thread-%d", hzInstanceName, seq.getAndIncrement()));
        }
    }


}
