/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.io.IOException;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

import io.kroxylicious.proxy.config.datetime.DurationSpec;

public class DurationSpecDeserializer extends StdScalarDeserializer<DurationSpec> {

    public static final JsonDeserializer<DurationSpec> INSTANCE = new DurationSpecDeserializer();

    private DurationSpecDeserializer() {
        super(DurationSpec.class);
    }

    @Override
    public DurationSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        if (p.hasToken(JsonToken.VALUE_STRING)) {
            return DurationSpec.parse(p.getText());
        }
        else {
            throw new JsonParseException(p, "Expected a string value");
        }
    }
}
