/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.FieldSpec;
import io.kroxylicious.krpccodegen.schema.FieldType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.StructSpec;
import io.kroxylicious.krpccodegen.schema.Versions;

import freemarker.template.DefaultObjectWrapper;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.Version;

/**
 * Wraps the java representation of the Kafka Model in a Apache FreeMaker
 * template object.
 */
public class KrpcSchemaObjectWrapper extends DefaultObjectWrapper {

    /**
     * Creates a schema object wrapper for the specified version.
     * @param version kafka model version.
     */
    public KrpcSchemaObjectWrapper(Version version) {
        super(version);
    }

    @Override
    public TemplateModel wrap(Object obj) throws TemplateModelException {
        if (obj instanceof TemplateModel templateModel) {
            return templateModel;
        }
        else if (obj instanceof MessageSpec messageSpec) {
            return new MessageSpecModel(this, messageSpec);
        }
        else if (obj instanceof FieldSpec fieldSpec) {
            return new FieldSpecModel(this, fieldSpec);
        }
        else if (obj instanceof FieldType fieldType) {
            return new FieldTypeModel(this, fieldType);
        }
        else if (obj instanceof StructSpec structSpec) {
            return new StructSpecModel(this, structSpec);
        }
        else if (obj instanceof Versions versions) {
            return new VersionsModel(this, versions);
        }
        return super.wrap(obj);
    }

    @Override
    public Object unwrap(TemplateModel tm) throws TemplateModelException {
        if (tm instanceof MessageSpecModel messageSpecModel) {
            return messageSpecModel.spec;
        }
        else if (tm instanceof FieldSpecModel fieldSpecModel) {
            return fieldSpecModel.spec;
        }
        else if (tm instanceof StructSpecModel structSpecModel) {
            return structSpecModel.spec;
        }
        else if (tm instanceof FieldTypeModel fieldTypeModel) {
            return fieldTypeModel.fieldType;
        }
        else if (tm instanceof VersionsModel versionsModel) {
            return versionsModel.versions;
        }
        else {
            return super.unwrap(tm);
        }
    }

    @Override
    public Object tryUnwrapTo(TemplateModel tm, Class<?> targetClass) throws TemplateModelException {
        if (tm instanceof MessageSpecModel messageSpecModel
                && targetClass.isInstance(messageSpecModel.spec)) {
            return messageSpecModel.spec;
        }
        else if (tm instanceof FieldSpecModel fieldSpecModel
                && targetClass.isInstance(fieldSpecModel.spec)) {
            return fieldSpecModel.spec;
        }
        else if (tm instanceof StructSpecModel structSpecModel
                && targetClass.isInstance(structSpecModel.spec)) {
            return structSpecModel.spec;
        }
        else if (tm instanceof FieldTypeModel fieldTypeModel
                && targetClass.isInstance(fieldTypeModel.fieldType)) {
            return fieldTypeModel.fieldType;
        }
        else if (tm instanceof VersionsModel versionsModel
                && targetClass.isInstance(versionsModel.versions)) {
            return versionsModel.versions;
        }
        else {
            return super.tryUnwrapTo(tm, targetClass);
        }
    }

}
