/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.MessageSpecPair;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

class MessageSpecPairModel implements TemplateHashModel, AdapterTemplateModel {
    final MessageSpecPair pair;
    final KrpcSchemaObjectWrapper wrapper;

    MessageSpecPairModel(KrpcSchemaObjectWrapper wrapper, MessageSpecPair pair) {
        this.wrapper = wrapper;
        this.pair = pair;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "name" -> wrapper.wrap(pair.name());
            case "apiKey" -> wrapper.wrap(pair.apiKey());
            case "request" -> wrapper.wrap(pair.request());
            case "response" -> wrapper.wrap(pair.response());
            case "hasResourceList" -> wrapper.wrap(pair.hasResourceList());
            case "hasAtLeastOneEntityField" -> wrapper.wrap((TemplateMethodModelEx) this::handleHasAtLeastOneEntityField);
            default -> throw new TemplateModelException(pair.getClass().getSimpleName() + " doesn't have property '" + key + "'");
        };
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return pair;
    }

    @SuppressWarnings("java:S3740") // The Freemaker API is in terms of raw Lists
    private boolean handleHasAtLeastOneEntityField(List args) throws TemplateModelException {
        var seq = ModelUtils.modelArgsToSimpleSequence(args, wrapper);
        var set = ModelUtils.asEnumSet(seq, EntityType.class);
        return pair.hasAtLeastOneEntityField(set);
    }

}
