/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.InputDecorator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TextNode;

import edu.umd.cs.findbugs.annotations.NonNull;

public final class TokenExpandingJsonFactoryWrapper {

    /**
     * create a new JsonFactory will is guaranteed to be of the same type and have the
     * same configuration of the input factory.  The returned factory will perform
     * token expansion on the value nodes of any parsed input.
     */
    public static <F extends JsonFactory> F wrap(@NonNull F factory) {
        var preprocessingObjectMapper = createPreprocessingObjectMapper(factory);

        var builder = factory.rebuild();
        builder.inputDecorator(new TokenExpandingInputDecorator(preprocessingObjectMapper));
        return (F) builder.build();
    }

    private static <F extends JsonFactory> ObjectMapper createPreprocessingObjectMapper(@NonNull F factory) {
        var preprocessingFactory = factory.rebuild().build();
        return new ObjectMapper(preprocessingFactory)
                .registerModule(new SimpleModule().addDeserializer(String.class, new TokenExpandingSerializer()));
    }

    private static class TokenExpandingSerializer extends JsonDeserializer<String> {
        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            var node = p.getCodec().readTree(p);

            String string = ((TextNode) node).asText();

            // this is where we will plug in sys property/env var replacement.

            // this POC uses commons-text just for illustrative purposes. I don't want
            // Kroxylicious to have a commons-text dependency. StringSubstitutor's is very capable, but that
            // makes the attack surface large. I worry about CVEs.
            return StringSubstitutor.createInterpolator().replace(string);
        }
    }

    private static class TokenExpandingInputDecorator extends InputDecorator {
        private final ObjectMapper preprocessingMapper;

        protected TokenExpandingInputDecorator(ObjectMapper preprocessingMapper) {
            this.preprocessingMapper = preprocessingMapper;
        }

        @Override
        public InputStream decorate(IOContext ctxt, InputStream in) throws IOException {
            var all = preprocessingMapper.readValue(in, Map.class);
            return new ByteArrayInputStream(preprocessingMapper.writeValueAsBytes(all));
        }

        @Override
        public InputStream decorate(IOContext ctxt, byte[] src, int offset, int length) throws IOException {
            var all = preprocessingMapper.readValue(src, offset, length, Map.class);
            return new ByteArrayInputStream(preprocessingMapper.writeValueAsBytes(all));
        }

        @Override
        public Reader decorate(IOContext ctxt, Reader r) throws IOException {
            var all = preprocessingMapper.readValue(r, Map.class);
            return new StringReader(preprocessingMapper.writeValueAsString(all));
        }
    }
}
