/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import io.kroxylicious.krpccodegen.schema.EntityType;

import freemarker.template.TemplateMethodModelEx;

/**
 * A custom FreeMarker function which obtains the API key (as an enum name) from a message spec, e.g. "CREATE_TOPICS".
 */
public class EntityTypeSetFactory implements TemplateMethodModelEx {

    /**
     * Constructs a EntityTypeSetFactory.
     */
    public EntityTypeSetFactory() {
        super();
    }

    @Override
    public Object exec(List arguments) {
        var entityTypes = new LinkedHashSet<>(arguments.size());
        arguments.forEach(argument -> Optional.of(argument)
                .map(String::valueOf)
                .map(EntityType::valueOf)
                .ifPresent(entityTypes::add));
        return entityTypes;
    }
}
