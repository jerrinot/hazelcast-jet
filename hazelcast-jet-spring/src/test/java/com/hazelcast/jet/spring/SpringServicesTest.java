package com.hazelcast.jet.spring;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;

import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static com.hazelcast.jet.spring.SpringServices.mapBatchUsingSpringBean;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"application-context-processor.xml"})
public class SpringServicesTest {

    @Resource(name = "jet-instance")
    private JetInstance jetInstance;

    @BeforeClass
    @AfterClass
    public static void start() {
        Jet.shutdownAll();
    }

    @Test
    public void testMapBatchUsingSpringBean() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1L, 2L, 3L, 4L, 5L))
                .apply(mapBatchUsingSpringBean("risk-calculator", RiskCalculator::calculateRisk))
                .writeTo(assertAnyOrder(asList(-1L, -2L, -3L, -4L, -5L)));

        jetInstance.newJob(pipeline).join();
    }

    @Test
    public void testMapStreamUsingSpringBean() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withNativeTimestamps(0)
                .map(SimpleEvent::sequence)
                .apply(SpringServices.mapStreamUsingSpringBean("risk-calculator", RiskCalculator::calculateRisk))
                .writeTo(assertCollectedEventually(10, c -> {
                    assertTrue(c.size() > 100);
                    c.forEach(i -> assertTrue(i <= 0));
                }));

        Job job = jetInstance.newJob(pipeline);
        assertJobCompleted(job);
    }

    private static void assertJobCompleted(Job job) {
        try {
            job.join();
            fail("expected CompletionException");
        } catch (CompletionException e) {
            assertTrue(e.getMessage().contains("AssertionCompletedException: Assertion passed successfully"));
        }
    }
}