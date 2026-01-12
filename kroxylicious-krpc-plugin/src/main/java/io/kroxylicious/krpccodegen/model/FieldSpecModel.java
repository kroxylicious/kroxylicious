/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.FieldSpec;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
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
        switch (key) {
            case "name":
                return wrapper.wrap(spec.name());
            case "fields":
                return wrapper.wrap(spec.fields());
            case "type":
                return wrapper.wrap(spec.type());
            case "typeString":
                return wrapper.wrap(spec.typeString());
            case "about":
                return wrapper.wrap(spec.about());
            case "entityType":
                return wrapper.wrap(spec.entityType());
            case "flexibleVersions":
                return wrapper.wrap(spec.flexibleVersions());
            case "flexibleVersionsString":
                return wrapper.wrap(spec.flexibleVersionsString());
            case "defaultString":
                return wrapper.wrap(spec.defaultString());
            case "ignorable":
                return wrapper.wrap(spec.ignorable());
            case "mapKey":
                return wrapper.wrap(spec.mapKey());
            case "nullableVersions":
                return wrapper.wrap(spec.nullableVersions());
            case "nullableVersionsString":
                return wrapper.wrap(spec.nullableVersionsString());
            case "tag":
                return wrapper.wrap(spec.tag());
            case "taggedVersions":
                return wrapper.wrap(spec.taggedVersions());
            case "tagInteger":
                return wrapper.wrap(spec.tagInteger());
            case "taggedVersionsString":
                return wrapper.wrap(spec.taggedVersionsString());
            case "versions":
                return wrapper.wrap(spec.versions());
            case "versionsString":
                return wrapper.wrap(spec.versionsString());
            case "zeroCopy":
                return wrapper.wrap(spec.zeroCopy());
        }
        throw new TemplateModelException(spec.getClass().getSimpleName() + " doesn't have property " + key);
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
