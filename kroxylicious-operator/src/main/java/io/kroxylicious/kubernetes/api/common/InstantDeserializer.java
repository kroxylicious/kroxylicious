/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class InstantDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(
                               JsonParser p,
                               DeserializationContext ctxt)
            throws IOException {
        return Instant.parse(p.getText());
    }
}
