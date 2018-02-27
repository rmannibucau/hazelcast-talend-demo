package com.github.rmannibucau.hazelcast.talend;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.talend.sdk.component.api.base.BufferizedProducerSupport;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastProcessorTest {
    @ClassRule
    public static final SimpleComponentRule COMPONENTS = new SimpleComponentRule(HazelcastProcessorTest.class.getPackage().getName());

    private static HazelcastInstance instance;

    @BeforeClass
    public static void startInstanceWithData() {
        instance = Hazelcast.newHazelcastInstance();
        instance.getMap(HazelcastProcessorTest.class.getSimpleName()).clear();
    }

    @AfterClass
    public static void stopInstance() {
        instance.getLifecycleService().shutdown();
    }

    @Test
    public void run() {
        System.setProperty("talend.beam.job.targetParallelism", "1"); // our code creates one hz lite instance per thread
        Job.components()
                .component("source", "HazelcastTest://Input")
                .component("output", "Hazelcast://Output?" +
                        "configuration.mapName=" + HazelcastProcessorTest.class.getSimpleName() + "&" +
                        "configuration.keyAttribute=key&" +
                        "configuration.valueAttribute=value")
            .connections()
                .from("source").to("output")
            .build()
            .run();
        // todo: while !true retry for 1mn (teardown issue on beam)
        assertEquals(100, instance.getMap(HazelcastProcessorTest.class.getSimpleName()).size());
    }

    @Emitter(family = "HazelcastTest", name = "Input")
    public static class LocalOutput implements Serializable {
        static final BufferizedProducerSupport<JsonObject> OBJECTS = new BufferizedProducerSupport<>(new Supplier
                <Iterator<JsonObject>>() {
            private volatile boolean done = false;
            @Override
            public synchronized Iterator<JsonObject> get() {
                if (done) {
                    return null;
                }
                done = true;
                final JsonBuilderFactory factory = Json.createBuilderFactory(emptyMap());
                return IntStream.range(0, 100)
                                .mapToObj(i -> factory.createObjectBuilder()
                                                      .add("key", "k_" + i)
                                                      .add("value", "v_" + i)
                                                      .build())
                                .collect(toList())
                                .iterator();
            }
        });

        @Producer
        public JsonObject next() {
            synchronized (OBJECTS) {
                return OBJECTS.next();
            }
        }
    }
}
