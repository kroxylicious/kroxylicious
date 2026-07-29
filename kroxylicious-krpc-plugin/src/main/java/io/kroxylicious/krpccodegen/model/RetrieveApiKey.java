/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;
import java.util.Locale;

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
        return toEnumConstantName(messageSpecModel.spec.name());
    }

    static String toEnumConstantName(String specName) {
        String baseName = specName.replaceFirst("(Request|Response)$", "");
        return baseName.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT);
    }

    @Override
    public Object exec(List arguments) {
        return retrieveApiKey((MessageSpecModel) arguments.getFirst());
    }
}
