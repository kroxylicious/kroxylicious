/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.FieldType;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateScalarModel;

class FieldTypeModel implements TemplateHashModel, TemplateScalarModel, AdapterTemplateModel {
    final FieldType fieldType;
    final KrpcSchemaObjectWrapper wrapper;

    FieldTypeModel(KrpcSchemaObjectWrapper wrapper, FieldType fieldType) {
        this.wrapper = wrapper;
        this.fieldType = fieldType;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        return switch (key) {
            case "isArray" -> wrapper.wrap(fieldType.isArray());
            case "isStruct" -> wrapper.wrap(fieldType.isStruct());
            case "isBytes" -> wrapper.wrap(fieldType.isBytes());
            case "canBeNullable" -> wrapper.wrap(fieldType.canBeNullable());
            case "fixedLength" -> wrapper.wrap(fieldType.fixedLength());
            case "isStructArray" -> wrapper.wrap(fieldType.isStructArray());
            case "isFloat" -> wrapper.wrap(fieldType.isFloat());
            case "isRecords" -> wrapper.wrap(fieldType.isRecords());
            case "isVariableLength" -> wrapper.wrap(fieldType.isVariableLength());
            case "serializationIsDifferentInFlexibleVersions" -> wrapper.wrap(fieldType.serializationIsDifferentInFlexibleVersions());
            case "elementType" -> wrapper.wrap(((FieldType.ArrayType) fieldType).elementType());
            case "elementName" -> wrapper.wrap(((FieldType.ArrayType) fieldType).elementName());
            default -> throw new TemplateModelException(fieldType.getClass().getSimpleName() + " doesn't have property " + key);
        };
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return fieldType;
    }

    @Override
    public String getAsString() {
        return fieldType.toString();
    }
}
