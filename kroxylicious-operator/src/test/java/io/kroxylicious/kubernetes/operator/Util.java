/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

public class Util {
    public static final YAMLMapper YAML_MAPPER = new YAMLMapper()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);

    private Util() {
    }

    public static KafkaProxy kafkaProxyFromString(String yaml) throws JsonProcessingException {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        return YAML_MAPPER.readValue(yaml, KafkaProxy.class);
    }

    public static KafkaProxy kafkaProxyFromResource(String name) throws IOException {
        // TODO should validate against the CRD schema, because the DependentResource
        // should never see an invalid resource in production
        return YAML_MAPPER.readValue(Util.class.getResource(name), KafkaProxy.class);
    }
}
