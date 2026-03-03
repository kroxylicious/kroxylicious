/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;

import freemarker.template.TemplateMethodModelEx;

/**
 * A custom FreeMarker function which obtains the API key (as an enum name) from a message spec, e.g. "CREATE_TOPICS".
 */
public class RetrieveApiKey implements TemplateMethodModelEx {

    /**
     * Constructs a RetrieveApiKey.
     */
    public RetrieveApiKey() {
        super();
    }

    private static String retrieveApiKey(MessageSpecModel messageSpecModel) {
        Short apiKey = messageSpecModel.spec.apiKey().orElseThrow();
        return ApiKeys.forId(apiKey).name();
    }

    @Override
    public Object exec(List arguments) {
        return retrieveApiKey((MessageSpecModel) arguments.get(0));
    }
}
