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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.FunctionEx;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.EarlyResultPolicy.OLDEST_ONLY;
import static com.hazelcast.jet.pipeline.Sinks.logger;
import static com.hazelcast.jet.pipeline.Sinks.noop;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DevSourcesTest extends PipelineStreamTestSupport {

    @Test
    public void fixedRate() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(DevSources.fixedRate(1, MICROSECONDS, UUID::randomUUID))
                .withIngestionTimestamps()
                .window(tumbling(1000))
                .aggregate(counting())
                .map(WindowResult::result)
                .drainTo(logger());

        jet().newJob(pipeline).join();
    }

    @Test
    public void earlyResultPolicies() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(DevSources.fixedRate(1, MILLISECONDS))
                .withIngestionTimestamps()
                .window(sliding(
                        HOURS.toMillis(1),
                        MINUTES.toMillis(1))
                        .setEarlyResultsPeriod(SECONDS.toMillis(1))
                        .setEarlyResultPolicy(OLDEST_ONLY)
                )
                .aggregate(counting())
                .peek()
                .drainTo(noop());

        jet().newJob(pipeline).join();
    }

    @Test
    public void ser() {
        Pipeline pipeline = Pipeline.create();

        FunctionEx<Object, Object> functionEx = (e -> e);
        IMapJet<Object, FunctionEx<Object, Object>> foo = jet().getMap("foo");
        foo.put(0, functionEx);

        System.out.println(foo.get(0).apply("foo"));
    }

    @Test
    public void average() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(DevSources.fixedRate(1, MILLISECONDS))
                .withIngestionTimestamps()
                .groupingKey(e -> ThreadLocalRandom.current().nextInt(10000))
                .window(sliding(
                        HOURS.toMillis(1),
                        MINUTES.toMillis(1))
                        .setEarlyResultsPeriod(SECONDS.toMillis(1))
                        .setEarlyResultPolicy(OLDEST_ONLY)
                )
                .aggregate(averagingLong(e -> e))
//                .peek()
                .drainTo(logger());

        jet().newJob(pipeline).join();
    }

    @Test
    public void batchSource() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(DevSources.of(1, 2, 3, 4, 5, 6, 7, 8, 9))
                .aggregate(AggregateOperations.averagingLong(i -> i))
                .drainTo(logger());

        jet().newJob(pipeline).join();
    }
}
