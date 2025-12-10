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
        if (obj instanceof TemplateModel tm) {
            return tm;
        }
        else if (obj instanceof MessageSpec ms) {
            return new MessageSpecModel(this, ms);
        }
        else if (obj instanceof FieldSpec fs) {
            return new FieldSpecModel(this, fs);
        }
        else if (obj instanceof FieldType ft) {
            return new FieldTypeModel(this, ft);
        }
        else if (obj instanceof StructSpec ss) {
            return new StructSpecModel(this, ss);
        }
        else if (obj instanceof Versions v) {
            return new VersionsModel(this, v);
        }
        return super.wrap(obj);
    }

    @Override
    public Object unwrap(TemplateModel tm) throws TemplateModelException {
        if (tm instanceof MessageSpecModel msm) {
            return msm.spec;
        }
        else if (tm instanceof FieldSpecModel fsm) {
            return fsm.spec;
        }
        else if (tm instanceof StructSpecModel ssm) {
            return ssm.spec;
        }
        else if (tm instanceof FieldTypeModel ftm) {
            return ftm.fieldType;
        }
        else if (tm instanceof VersionsModel vm) {
            return vm.versions;
        }
        else {
            return super.unwrap(tm);
        }
    }

    @Override
    public Object tryUnwrapTo(TemplateModel tm, Class<?> targetClass) throws TemplateModelException {
        if (tm instanceof MessageSpecModel msm
                && targetClass.isInstance(msm.spec)) {
            return msm.spec;
        }
        else if (tm instanceof FieldSpecModel fsm
                && targetClass.isInstance(fsm.spec)) {
            return fsm.spec;
        }
        else if (tm instanceof StructSpecModel ssm
                && targetClass.isInstance(ssm.spec)) {
            return ssm.spec;
        }
        else if (tm instanceof FieldTypeModel ftm
                && targetClass.isInstance(ftm.fieldType)) {
            return ftm.fieldType;
        }
        else if (tm instanceof VersionsModel vm
                && targetClass.isInstance(vm.versions)) {
            return vm.versions;
        }
        else {
            return super.tryUnwrapTo(tm, targetClass);
        }
    }

}
