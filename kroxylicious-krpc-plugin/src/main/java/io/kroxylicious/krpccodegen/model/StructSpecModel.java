/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.StructSpec;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

class StructSpecModel implements TemplateHashModel, AdapterTemplateModel {
    final StructSpec spec;
    final KrpcSchemaObjectWrapper wrapper;

    StructSpecModel(KrpcSchemaObjectWrapper wrapper, StructSpec ms) {
        this.wrapper = wrapper;
        this.spec = ms;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "name" -> wrapper.wrap(spec.name());
            case "fields" -> wrapper.wrap(spec.fields());
            case "versions" -> wrapper.wrap(spec.versions());
            case "versionsString" -> wrapper.wrap(spec.versionsString());
            case "hasKeys" -> wrapper.wrap(spec.hasKeys());
            default -> throw new TemplateModelException(spec.getClass().getSimpleName() + " doesn't have property " + key);
        };
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return spec;
    }
}
