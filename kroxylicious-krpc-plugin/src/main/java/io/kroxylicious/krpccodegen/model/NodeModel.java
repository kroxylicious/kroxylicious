/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.Node;

import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

public class NodeModel implements TemplateHashModel {
    private final KrpcSchemaObjectWrapper wrapper;
    private final Node node;

    public NodeModel(KrpcSchemaObjectWrapper wrapper, Node node) {
        this.wrapper = wrapper;
        this.node = node;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "hasAtLeastOneEntityField" -> wrapper.wrap(node.hasAtLeastOneEntityField());
            case "orderedVersions" -> wrapper.wrap(node.orderedVersions());
            case "entities" -> wrapper.wrap(node.entities());
            default -> throw new TemplateModelException(node.getClass().getSimpleName() + " doesn't have property " + key);
        };
    }

    @Override
    public boolean isEmpty() throws TemplateModelException {
        return false;
    }
}
