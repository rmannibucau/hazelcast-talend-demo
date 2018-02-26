package com.github.rmannibucau.hazelcast.talend;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import javax.json.JsonObject;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.manager.chain.Job;

public class HazelcastMapperTest {
    @ClassRule
    public static final SimpleComponentRule COMPONENTS = new SimpleComponentRule(HazelcastMapperTest.class.getPackage().getName());

    private static HazelcastInstance instance;

    @BeforeClass
    public static void startInstanceWithData() {
        instance = Hazelcast.newHazelcastInstance();
        final IMap<Object, Object> map = instance.getMap(HazelcastMapperTest.class.getSimpleName());
        IntStream.range(0, 100).forEach(i -> map.put("test_" + i, "value #" + i));
    }

    @AfterClass
    public static void stopInstance() {
        instance.getLifecycleService().shutdown();
    }

    @Test
    public void run() {
        Job.components()
                .component("source", "Hazelcast://Input?configuration.mapName=" + HazelcastMapperTest.class.getSimpleName())
                .component("output", "HazelcastTest://collector")
            .connections()
                .from("source").to("output")
            .build()
            .run();
        // todo: while !true retry for 1mn (teardown issue on beam)
        final List<JsonObject> outputs = LocalOutput.OBJECTS;
        assertEquals(100, outputs.size());
        // todo
    }

    @Processor(family = "HazelcastTest", name = "collector")
    public static class LocalOutput implements Serializable {
        static final List<JsonObject> OBJECTS = new ArrayList<>();

        @ElementListener
        public void onNext(final JsonObject object) {
            synchronized (OBJECTS) {
                OBJECTS.add(object);
            }
        }
    }
}
