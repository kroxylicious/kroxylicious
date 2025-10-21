/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.FieldSpec;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

class FieldSpecModel implements TemplateHashModel, AdapterTemplateModel {
    final FieldSpec spec;
    final KrpcSchemaObjectWrapper wrapper;

    FieldSpecModel(KrpcSchemaObjectWrapper wrapper, FieldSpec ms) {
        this.wrapper = wrapper;
        this.spec = ms;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "name" -> wrapper.wrap(spec.name());
            case "fields" -> wrapper.wrap(spec.fields());
            case "type" -> wrapper.wrap(spec.type());
            case "typeString" -> wrapper.wrap(spec.typeString());
            case "about" -> wrapper.wrap(spec.about());
            case "entityType" -> wrapper.wrap(spec.entityType());
            case "hasAtLeastOneEntityField" -> wrapper.wrap((TemplateMethodModelEx) this::handleHasAtLeastOneEntityField);
            case "flexibleVersions" -> wrapper.wrap(spec.flexibleVersions());
            case "flexibleVersionsString" -> wrapper.wrap(spec.flexibleVersionsString());
            case "defaultString" -> wrapper.wrap(spec.defaultString());
            case "ignorable" -> wrapper.wrap(spec.ignorable());
            case "mapKey" -> wrapper.wrap(spec.mapKey());
            case "nullableVersions" -> wrapper.wrap(spec.nullableVersions());
            case "nullableVersionsString" -> wrapper.wrap(spec.nullableVersionsString());
            case "tag" -> wrapper.wrap(spec.tag());
            case "taggedVersions" -> wrapper.wrap(spec.taggedVersions());
            case "tagInteger" -> wrapper.wrap(spec.tagInteger());
            case "taggedVersionsString" -> wrapper.wrap(spec.taggedVersionsString());
            case "versions" -> wrapper.wrap(spec.versions());
            case "versionsString" -> wrapper.wrap(spec.versionsString());
            case "zeroCopy" -> wrapper.wrap(spec.zeroCopy());
            default -> throw new TemplateModelException(spec.getClass().getSimpleName() + " doesn't have property " + key);
        };
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    private boolean handleHasAtLeastOneEntityField(List args) throws TemplateModelException {
        var seq = ModelUtils.modelArgsToSimpleSequence(args, wrapper);
        var set = ModelUtils.asEnumSet(seq, EntityType.class);
        return spec.hasAtLeastOneEntityField(set);
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return spec;
    }
}
