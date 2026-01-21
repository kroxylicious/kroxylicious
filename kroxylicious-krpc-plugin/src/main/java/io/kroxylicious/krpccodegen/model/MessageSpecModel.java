/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;

import freemarker.ext.beans.GenericObjectModel;
import freemarker.template.AdapterTemplateModel;
import freemarker.template.ObjectWrapperAndUnwrapper;
import freemarker.template.SimpleSequence;
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
        var seq = argsToSimpleSequence(args);
        var set = getTypeHashSet(seq);
        return spec.hasAtLeastOneEntityField(set);
    }

    private List<Short> handleIntersectedVersionsForEntityFields(List args) throws TemplateModelException {
        var seq = argsToSimpleSequence(args);
        var set = getTypeHashSet(seq);
        return spec.intersectedVersionsForEntityFields(set);
    }

    private SimpleSequence argsToSimpleSequence(List args) throws TemplateModelException {
        var seq = new SimpleSequence(wrapper);
        for (Object objOrSeq : args) {
            if (objOrSeq instanceof SimpleSequence ss) {
                for (int i = 0; i < ss.size(); i++) {
                    var obj = ss.get(i);
                    seq.add(obj);
                }
            }
            else if (objOrSeq instanceof GenericObjectModel gom) {
                seq.add(gom);
            }
            else {
                throw new TemplateModelException("Unsupported argument type " + objOrSeq.getClass().getName() + " found in arguments.");
            }
        }
        return seq;
    }

    private static Set<EntityType> getTypeHashSet(SimpleSequence seq) throws TemplateModelException {
        var ow = (ObjectWrapperAndUnwrapper) seq.getObjectWrapper();
        var set = new HashSet<EntityType>(seq.size());
        for (int i = 0; i < seq.size(); i++) {
            try {
                TemplateModel obj = seq.get(i);
                var unwrapped = ow.unwrap(obj);
                set.add(unwrapped instanceof EntityType et ? et : EntityType.valueOf(String.valueOf(unwrapped)));
            }
            catch (TemplateModelException e) {
                throw new TemplateModelException("Failed to unwrap template model object at index " + i, e);
            }
        }
        return set;
    }
}
