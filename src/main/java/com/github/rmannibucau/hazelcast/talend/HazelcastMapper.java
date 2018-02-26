package com.github.rmannibucau.hazelcast.talend;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.talend.sdk.component.api.component.Icon.IconType.DEFAULT;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.bind.Jsonb;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;

import lombok.Data;

@Data
@Version
@Icon(DEFAULT) // todo
@PartitionMapper(name = "Input")
public class HazelcastMapper implements Serializable {
    private final HazelcastConfiguration configuration;
    private final JsonBuilderFactory jsonFactory;
    private final Jsonb jsonb;
    private final HazelcastService service;

    private final Collection<String> members;

    private transient HazelcastInstance instance;
    private transient IExecutorService executorService;

    // framework API
    public HazelcastMapper(@Option("configuration") final HazelcastConfiguration configuration,
                           final JsonBuilderFactory jsonFactory,
                           final Jsonb jsonb,
                           final HazelcastService service) {
        this(configuration, jsonFactory, jsonb, service, emptyList());
    }

    // internal
    protected HazelcastMapper(final HazelcastConfiguration configuration,
                              final JsonBuilderFactory jsonFactory,
                              final Jsonb jsonb,
                              final HazelcastService service,
                              final Collection<String> members) {
        this.configuration = configuration;
        this.jsonFactory = jsonFactory;
        this.jsonb = jsonb;
        this.service = service;
        this.members = members;
    }

    // serialization
    protected HazelcastMapper() {
        this(null, null, null, null);
    }

    @PostConstruct
    public void createInstance() {
        instance = service.findInstance(configuration.newConfig());
    }

    @PreDestroy
    public void destroyInstance() {
        instance.getLifecycleService().shutdown();
        executorService = null;
    }

    @Assessor
    public long estimateSize() {
        return getSizeByMembers().values().stream()
                .mapToLong(this::getFutureValue)
                .sum();
    }

    @Split
    public List<HazelcastMapper> computeExecutionPlan(@PartitionSize final long bundleSize) {
        final List<HazelcastMapper> partitionPlan = new ArrayList<>();

        final Collection<Member> members = new ArrayList<>();
        long current = 0;
        for (final Map.Entry<Member, Future<Long>> entries : getSizeByMembers().entrySet()) {
            final long memberSize = getFutureValue(entries.getValue());
            if (members.isEmpty()) {
                members.add(entries.getKey());
                current += memberSize;
            } else if (current + memberSize > bundleSize) {
                partitionPlan.add(new HazelcastMapper(configuration, jsonFactory, jsonb, service, toIdentifiers(members)));
                // reset current iteration
                members.clear();
                current = 0;
            }
        }
        if (!members.isEmpty()) {
            partitionPlan.add(new HazelcastMapper(configuration, jsonFactory, jsonb, service, toIdentifiers(members)));
        }

        if (partitionPlan.isEmpty()) { // just execute this if no plan (= no distribution)
            partitionPlan.add(this);
        }
        return partitionPlan;
    }

    @Emitter
    public HazelcastSource createSource() {
        return new HazelcastSource(configuration, jsonFactory, jsonb, service, members);
    }

    private Set<String> toIdentifiers(final Collection<Member> members) {
        return members.stream().map(Member::getUuid).collect(toSet());
    }

    private long getFutureValue(final Future<Long> future) {
        try {
            return future.get(configuration.getTimeout(), SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (final ExecutionException | TimeoutException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // note: this works if this jar is deployed on the hz cluster
    private Map<Member, Future<Long>> getSizeByMembers() {
        final String mapName = configuration.getMapName();
        final IExecutorService executorService = getExecutorService();
        final SerializableTask<Long> sizeComputation = new SerializableTask<Long>() {
            @Override
            public Long call() throws Exception {
                return localInstance.getMap(mapName).getLocalMapStats().getHeapCost();
            }
        };
        if (members.isEmpty()) { // == work on all the cluster
            return executorService.submitToAllMembers(sizeComputation);
        }
        final Set<Member> members = instance.getCluster().getMembers().stream()
                .filter(m -> this.members.contains(m.getUuid()))
                .collect(toSet());
        return executorService.submitToMembers(sizeComputation, members);
    }

    private IExecutorService getExecutorService() {
        return executorService == null ?
                executorService = instance.getExecutorService(configuration.getExecutorService()) :
                executorService;
    }
}
