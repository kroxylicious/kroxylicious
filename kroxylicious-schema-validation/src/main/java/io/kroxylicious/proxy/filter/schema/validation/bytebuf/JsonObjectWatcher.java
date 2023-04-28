/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

interface JsonObjectWatcher {

    JsonObjectWatcher NOOP = new JsonObjectWatcher() {
    };

    default void onToken(JsonParser parser, JsonToken token) throws IOException {
    }

    ;
}
