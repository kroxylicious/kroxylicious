/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;

import io.kroxylicious.krpccodegen.schema.ApiSpec;
import io.kroxylicious.krpccodegen.schema.EntityType;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

class ApiSpecModel implements TemplateHashModel, AdapterTemplateModel {
    final ApiSpec spec;
    final KrpcSchemaObjectWrapper wrapper;

    ApiSpecModel(KrpcSchemaObjectWrapper wrapper, ApiSpec spec) {
        this.wrapper = wrapper;
        this.spec = spec;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "name" -> wrapper.wrap(spec.name());
            case "apiKey" -> wrapper.wrap(spec.apiKey());
            case "request" -> wrapper.wrap(spec.request());
            case "response" -> wrapper.wrap(spec.response());
            case "hasResourceList" -> wrapper.wrap(spec.hasResourceList());
            case "hasAtLeastOneEntityField" -> wrapper.wrap((TemplateMethodModelEx) this::handleHasAtLeastOneEntityField);
            default -> throw new TemplateModelException(spec.getClass().getSimpleName() + " doesn't have property '" + key + "'");
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

    @SuppressWarnings("java:S3740") // The Freemaker API is in terms of raw Lists
    private boolean handleHasAtLeastOneEntityField(List args) throws TemplateModelException {
        var seq = ModelUtils.modelArgsToSimpleSequence(args, wrapper);
        var set = ModelUtils.asEnumSet(seq, EntityType.class);
        return spec.hasAtLeastOneEntityField(set);
    }

}
