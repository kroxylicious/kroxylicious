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
 * A custom FreeMarker function which converts a list of entity type names into set of {@link EntityType}s.
 */
public class EntityTypeSetFactory implements TemplateMethodModelEx {

    /**
     * Constructs a EntityTypeSetFactory.
     */
    public EntityTypeSetFactory() {
        super();
    }

    @Override
    @SuppressWarnings("unchecked") // Freemarker uses raw lists for its arguments
    public Object exec(List arguments) {
        var entityTypes = new LinkedHashSet<>(arguments.size());
        arguments.forEach(argument -> Optional.of(argument)
                .map(String::valueOf)
                .map(EntityType::valueOf)
                .ifPresent(entityTypes::add));
        return entityTypes;
    }
}
