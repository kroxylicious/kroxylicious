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
        switch (key) {
            case "isArray":
                return wrapper.wrap(fieldType.isArray());
            case "isStruct":
                return wrapper.wrap(fieldType.isStruct());
            case "isBytes":
                return wrapper.wrap(fieldType.isBytes());
            case "canBeNullable":
                return wrapper.wrap(fieldType.canBeNullable());
            case "fixedLength":
                return wrapper.wrap(fieldType.fixedLength());
            case "isStructArray":
                return wrapper.wrap(fieldType.isStructArray());
            case "isFloat":
                return wrapper.wrap(fieldType.isFloat());
            case "isRecords":
                return wrapper.wrap(fieldType.isRecords());
            case "isVariableLength":
                return wrapper.wrap(fieldType.isVariableLength());
            case "serializationIsDifferentInFlexibleVersions":
                return wrapper.wrap(fieldType.serializationIsDifferentInFlexibleVersions());
            case "elementType":
                return wrapper.wrap(((FieldType.ArrayType) fieldType).elementType());
            case "elementName":
                return wrapper.wrap(((FieldType.ArrayType) fieldType).elementName());
        }
        throw new TemplateModelException(fieldType.getClass().getSimpleName() + " doesn't have property " + key);
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
