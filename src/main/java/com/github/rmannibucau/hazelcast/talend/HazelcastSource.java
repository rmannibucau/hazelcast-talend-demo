package com.github.rmannibucau.hazelcast.talend;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.Jsonb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

import org.talend.sdk.component.api.base.BufferizedProducerSupport;
import org.talend.sdk.component.api.input.Producer;

import lombok.Data;

@Data
public class HazelcastSource implements Serializable {
    private final HazelcastConfiguration configuration;
    private final JsonBuilderFactory jsonFactory;
    private final Jsonb jsonb;
    private final HazelcastService service;
    private final Collection<String> members;

    private transient HazelcastInstance instance;
    private transient BufferizedProducerSupport<JsonObject> buffer;

    @PostConstruct
    public void createInstance() {
        instance = service.findInstance(configuration.newConfig());
        final Iterator<Member> memberIterators = instance.getCluster().getMembers().stream()
                .filter(m -> members.isEmpty() || members.contains(m.getUuid()))
                .collect(toSet())
                .iterator();
        buffer = new BufferizedProducerSupport<>(() -> {
            if (!memberIterators.hasNext()) {
                return null;
            }
            final Member member = memberIterators.next();
            // note: this works if this jar is deployed on the hz cluster
            try {
                return instance.getExecutorService(configuration.getExecutorService())
                        .submitToMember(new SerializableTask<Map<String, String>>() {
                            @Override
                            public Map<String, String> call() throws Exception {
                                final IMap<Object, Object> map = localInstance.getMap(configuration.getMapName());
                                final Set<?> keys = map.localKeySet();
                                return keys.stream().collect(toMap(jsonb::toJson, e -> jsonb.toJson(map.get(e))));
                            }
                        }, member).get(configuration.getTimeout(), SECONDS).entrySet().stream()
                        .map(entry -> {
                            final JsonObjectBuilder builder = jsonFactory.createObjectBuilder();
                            if (entry.getKey().startsWith("{")) {
                                builder.add("key", jsonb.fromJson(entry.getKey(), JsonObject.class));
                            } else { // plain string
                                builder.add("key", entry.getKey());
                            }
                            if (entry.getValue().startsWith("{")) {
                                builder.add("value", jsonb.fromJson(entry.getValue(), JsonObject.class));
                            } else { // plain string
                                builder.add("value", entry.getValue());
                            }
                            return builder.build();
                        })
                        .collect(toList())
                        .iterator();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (final ExecutionException | TimeoutException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    @Producer
    public JsonObject next() {
        return buffer.next();
    }

    @PreDestroy
    public void destroyInstance() {
        instance.getLifecycleService().shutdown();
    }
}
