package com.github.rmannibucau.hazelcast.talend;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

public abstract class SerializableTask<T> implements Callable<T>, Serializable, HazelcastInstanceAware {
    protected transient HazelcastInstance localInstance;

    @Override
    public void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
        localInstance = hazelcastInstance;
    }
}
