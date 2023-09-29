/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class MicrometerPlugin extends Plugin<MicrometerPlugin.MicrometerFactory> {
    public static class MicrometerFactory {
        public Class<?> micrometerType() {
            return null;
        }

        public Class<?> configType() {
            return null;
        }
        // TODO replace this with Rob's stuff
    }


    private final String type;
    private final JsonNode config;

    public MicrometerPlugin(@JsonProperty(required = true) String type,
                            JsonNode config) {
        super(MicrometerFactory.class);
        this.type = type;
        this.config = config;
    }

    public String type() {
        return type;
    }

    public JsonNode config() {
        return config;
    }
}
