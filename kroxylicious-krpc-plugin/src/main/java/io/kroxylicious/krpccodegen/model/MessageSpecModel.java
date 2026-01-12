/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.MessageSpec;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
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
        switch (key) {
            case "struct":
                return wrapper.wrap(spec.struct());
            case "name":
                return wrapper.wrap(spec.name());
            case "validVersions":
                return wrapper.wrap(spec.validVersions());
            case "validVersionsString":
                return wrapper.wrap(spec.validVersionsString());
            case "fields":
                return wrapper.wrap(spec.fields());
            case "commonStructs":
                return wrapper.wrap(spec.commonStructs());
            case "flexibleVersions":
                return wrapper.wrap(spec.flexibleVersions());
            case "flexibleVersionsString":
                return wrapper.wrap(spec.flexibleVersionsString());
            case "apiKey":
                return wrapper.wrap(spec.apiKey());
            case "latestVersionUnstable":
                return wrapper.wrap(spec.latestVersionUnstable());
            case "type":
                return wrapper.wrap(spec.type());
            case "listeners":
                return wrapper.wrap(spec.listeners());
            case "dataClassName":
                return wrapper.wrap(spec.dataClassName());

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
