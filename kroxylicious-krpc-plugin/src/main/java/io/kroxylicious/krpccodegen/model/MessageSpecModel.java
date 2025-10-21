/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

class MessageSpecModel implements TemplateHashModel, AdapterTemplateModel {
    final MessageSpec spec;
    final KrpcSchemaObjectWrapper wrapper;

    MessageSpecModel(KrpcSchemaObjectWrapper wrapper, MessageSpec spec) {
        this.wrapper = wrapper;
        this.spec = spec;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "struct" -> wrapper.wrap(spec.struct());
            case "name" -> wrapper.wrap(spec.name());
            case "validVersions" -> wrapper.wrap(spec.validVersions());
            case "validVersionsString" -> wrapper.wrap(spec.validVersionsString());
            case "fields" -> wrapper.wrap(spec.fields());
            case "commonStructs" -> wrapper.wrap(spec.commonStructs());
            case "flexibleVersions" -> wrapper.wrap(spec.flexibleVersions());
            case "flexibleVersionsString" -> wrapper.wrap(spec.flexibleVersionsString());
            case "apiKey" -> wrapper.wrap(spec.apiKey());
            case "latestVersionUnstable" -> wrapper.wrap(spec.latestVersionUnstable());
            case "type" -> wrapper.wrap(spec.type());
            case "listeners" -> wrapper.wrap(spec.listeners());
            case "dataClassName" -> wrapper.wrap(spec.dataClassName());
            case "hasAtLeastOneEntityField" -> wrapper.wrap((TemplateMethodModelEx) this::handleHasAtLeastOneEntityField);
            case "intersectedVersionsForEntityFields" -> wrapper.wrap((TemplateMethodModelEx) this::handleIntersectedVersionsForEntityFields);
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

    private boolean handleHasAtLeastOneEntityField(List args) throws TemplateModelException {
        var seq = ModelUtils.modelArgsToSimpleSequence(args, wrapper);
        var set = ModelUtils.asEnumSet(seq, EntityType.class);
        return spec.hasAtLeastOneEntityField(set);
    }

    private List<Short> handleIntersectedVersionsForEntityFields(List args) throws TemplateModelException {
        var seq = ModelUtils.modelArgsToSimpleSequence(args, wrapper);
        var set = ModelUtils.asEnumSet(seq, EntityType.class);
        return spec.intersectedVersionsForEntityFields(set);
    }

}
