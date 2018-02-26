package com.github.rmannibucau.hazelcast.talend;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.talend.sdk.component.api.service.Service;

@Service
public class HazelcastService {
    public HazelcastInstance findInstance(final Config config) {
        return Hazelcast.newHazelcastInstance(config);
    }
}
