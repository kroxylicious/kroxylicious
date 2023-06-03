/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import io.kroxylicious.proxy.service.HostPort;

public class ConfigParser {

    private final ObjectMapper mapper = createObjectMapper();

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

    public String toYaml(Configuration configuration) {
        try {
            return mapper.writeValueAsString(configuration);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static ObjectMapper createObjectMapper() {
        return new ObjectMapper(new YAMLFactory())
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new SimpleModule().addSerializer(HostPort.class, new ToStringSerializer()))
                .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
                .setVisibility(PropertyAccessor.CREATOR, Visibility.ANY)
                .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }
}
