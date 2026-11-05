/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import io.kroxylicious.krpccodegen.schema.ApiSpec;
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
 * Wraps the java representation of the Kafka Model in an Apache FreeMaker
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
        return switch (obj) {
            case TemplateModel templateModel -> templateModel;
            case MessageSpec messageSpec -> new MessageSpecModel(this, messageSpec);
            case ApiSpec apiSpec -> new ApiSpecModel(this, apiSpec);
            case FieldSpec fieldSpec -> new FieldSpecModel(this, fieldSpec);
            case FieldType fieldType -> new FieldTypeModel(this, fieldType);
            case StructSpec structSpec -> new StructSpecModel(this, structSpec);
            case Versions versions -> new VersionsModel(this, versions);
            case null, default -> super.wrap(obj);
        };
    }

    @Override
    public Object unwrap(TemplateModel tm) throws TemplateModelException {
        return switch (tm) {
            case MessageSpecModel messageSpecModel -> messageSpecModel.spec;
            case ApiSpecModel apiSpecModel -> apiSpecModel.spec;
            case FieldSpecModel fieldSpecModel -> fieldSpecModel.spec;
            case StructSpecModel structSpecModel -> structSpecModel.spec;
            case FieldTypeModel fieldTypeModel -> fieldTypeModel.fieldType;
            case VersionsModel versionsModel -> versionsModel.versions;
            case null, default -> super.unwrap(tm);
        };
    }

    @Override
    public Object tryUnwrapTo(TemplateModel tm, Class<?> targetClass) throws TemplateModelException {
        return switch (tm) {
            case MessageSpecModel messageSpecModel when targetClass.isInstance(messageSpecModel.spec) -> messageSpecModel.spec;
            case FieldSpecModel fieldSpecModel when targetClass.isInstance(fieldSpecModel.spec) -> fieldSpecModel.spec;
            case StructSpecModel structSpecModel when targetClass.isInstance(structSpecModel.spec) -> structSpecModel.spec;
            case FieldTypeModel fieldTypeModel when targetClass.isInstance(fieldTypeModel.fieldType) -> fieldTypeModel.fieldType;
            case VersionsModel versionsModel when targetClass.isInstance(versionsModel.versions) -> versionsModel.versions;
            case null, default -> super.tryUnwrapTo(tm, targetClass);
        };
    }

}
