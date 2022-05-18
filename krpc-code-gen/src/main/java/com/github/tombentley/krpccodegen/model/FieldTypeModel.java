/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tombentley.krpccodegen.model;

import com.github.tombentley.krpccodegen.schema.FieldType;

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateScalarModel;

public class FieldTypeModel implements TemplateHashModel, TemplateScalarModel, AdapterTemplateModel {
    final FieldType fieldType;
    final KrpcSchemaObjectWrapper wrapper;

    public FieldTypeModel(KrpcSchemaObjectWrapper wrapper, FieldType fieldType) {
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
    public boolean isEmpty() throws TemplateModelException {
        return false;
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return fieldType;
    }

    @Override
    public String getAsString() throws TemplateModelException {
        return fieldType.toString();
    }
}
