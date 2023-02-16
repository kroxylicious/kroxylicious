/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

public class ConfigParser {

    private final ObjectMapper mapper;

    public ConfigParser() {
        this.mapper = new ObjectMapper(new YAMLFactory())
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
                .setVisibility(PropertyAccessor.CREATOR, Visibility.ANY)
                .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false);
    }

    public Configuration parseConfiguration(String configuration) {

        try {
            return mapper.readValue(configuration, Configuration.class);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Couldn't parse configuration", e);
        }
    }

    public Configuration parseConfiguration(InputStream configuration) {

        try {
            return mapper.readValue(configuration, Configuration.class);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Couldn't parse configuration", e);
        }
    }
}
