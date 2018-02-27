package com.github.rmannibucau.hazelcast.talend;

import static lombok.AccessLevel.NONE;

import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;

import org.talend.sdk.component.api.configuration.Option;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class HazelcastConfiguration implements Serializable {
    @Option
    private String hazelcastXml;

    @Option
    private long timeout = 60;

    @Option
    private String mapName;

    @Option
    private String keyAttribute;

    @Option
    private String valueAttribute;

    @Option
    private String executorService = "default";

    @Setter(NONE)
    @Getter(NONE)
    private transient volatile Config config;

    Config newConfig() {
        return (hazelcastXml == null ?
                new XmlConfigBuilder().build() :
                new InMemoryXmlConfig(hazelcastXml))
                .setLiteMember(true)
                .setInstanceName(getClass().getSimpleName() + "_" + UUID.randomUUID().toString())
                .setClassLoader(Thread.currentThread().getContextClassLoader());
    }
}
