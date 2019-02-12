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

package com.hazelcast.jet.impl.util;

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Windows-specific hack to enable high resolution timer.
 * This is needed as some versions of Windows use very coarse grained timer: When you request a thread to sleep for
 * just a few microseconds it actually sleeps +- 16ms. This has very undesirable impact on IDLE strategies:
 * Threads running tasklets are blocked for 16ms and this result in poor throughput and CPU utilization.
 *
 * See the workaround described at https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6435126
 * for rationale behind this hack.
 *
 */
public final class WindowsTimerHack {
    private static final String OS_NAME = System.getProperty("os.name");
    private static final boolean IS_WINDOWS = OS_NAME != null && OS_NAME.toLowerCase().contains("windows");
    private static final boolean WINDOWS_COARSE_GRAINED_TIMER = isWindowsCoarseGrainedTimer();

    private static final long PARK_REQUEST_NANOS = MICROSECONDS.toNanos(60);
    private static final long THRESHOLD_NANOS = MILLISECONDS.toNanos(5);
    private static final int  ITERATIONS = 20;

    private final SleepyThread currentThread;
    private volatile boolean stopRequested;

    public WindowsTimerHack(String hzInstanceName, HazelcastProperties properties) {
        if (isEnabled(properties)) {
            currentThread = new SleepyThread();
        } else {
            currentThread = null;
            return;
        }
        String threadName = String.format("hz.%s.jet.windows-highres-timer.thread", hzInstanceName);
        currentThread.setName(threadName);
        currentThread.setDaemon(true);
        currentThread.start();
    }

    private boolean isEnabled(HazelcastProperties properties) {
        if (!IS_WINDOWS) {
            return false;
        }
        String hackPropertyValue = properties.getString(JetGroupProperty.WINDOWS_HIGHRES_TIMER_HACK);
        if ("on".equals(hackPropertyValue)) {
            return true;
        } else if ("off".equals(hackPropertyValue)) {
            return false;
        } else if ("auto".equals(hackPropertyValue)) {
            return WINDOWS_COARSE_GRAINED_TIMER;
        } else {
            throw new ConfigurationException("Property " + JetGroupProperty.WINDOWS_HIGHRES_TIMER_HACK + " has illegal"
                    + " value: '" + hackPropertyValue + "'. Possible values: on/off/auto");
        }
    }

    private static boolean isWindowsCoarseGrainedTimer() {
        if (!IS_WINDOWS) {
            return false;
        }
        for (int i = 0; i < ITERATIONS; i++) {
            long before = System.nanoTime();
            LockSupport.parkNanos(PARK_REQUEST_NANOS);
            long actualParkingTimeNanos = System.nanoTime() - before;
            if (actualParkingTimeNanos < THRESHOLD_NANOS) {
                // ok, at least 1 parking duration was short enough, probably won't need to active the hack
                return false;
            }
        }
        return true;
    }

    public void shutdown() {
        stopRequested = true;
        if (currentThread != null) {
            currentThread.interrupt();
        }
    }


    private final class SleepyThread extends Thread {
        @Override
        public void run() {
            while (!WindowsTimerHack.this.stopRequested) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    //
                }
            }
        }
    }
}
