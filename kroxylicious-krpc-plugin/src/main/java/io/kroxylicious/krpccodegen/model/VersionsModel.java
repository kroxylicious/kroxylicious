/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.Versions;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.SimpleNumber;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateScalarModel;
import freemarker.template.TemplateSequenceModel;

class VersionsModel implements TemplateHashModel, TemplateScalarModel, TemplateSequenceModel, AdapterTemplateModel {
    final Versions versions;
    final KrpcSchemaObjectWrapper wrapper;

    VersionsModel(KrpcSchemaObjectWrapper wrapper, Versions versions) {
        this.wrapper = wrapper;
        this.versions = versions;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        switch (key) {
            case "highest":
                return wrapper.wrap(versions.highest());
            case "lowest":
                return wrapper.wrap(versions.lowest());
            case "intersect":
                return wrapper.wrap((TemplateMethodModelEx) args -> {
                    Object o = args.get(0);
                    return versions.intersect(((VersionsModel) o).versions);
                });
            case "contains":
                return wrapper.wrap((TemplateMethodModelEx) args -> {
                    Object o = args.get(0);
                    return versions.contains(((SimpleNumber) o).getAsNumber().shortValue());
                });
        }
        throw new TemplateModelException(versions.getClass().getSimpleName() + " doesn't have property " + key);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return versions;
    }

    @Override
    public String getAsString() {
        return versions.toString();
    }

    @Override
    public TemplateModel get(int index) throws TemplateModelException {
        return wrapper.wrap(versions.lowest() + index);
    }

    @Override
    public int size() {
        return versions.empty() ? 0 : versions.highest() - versions.lowest() + 1;
    }
}
