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
package io.kroxylicious.krpccodegen.model;

import freemarker.template.DefaultObjectWrapper;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.Version;
import io.kroxylicious.krpccodegen.schema.FieldSpec;
import io.kroxylicious.krpccodegen.schema.FieldType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.StructSpec;
import io.kroxylicious.krpccodegen.schema.Versions;

public class KrpcSchemaObjectWrapper extends DefaultObjectWrapper {

    public KrpcSchemaObjectWrapper(Version version) {
        super(version);
    }

    @Override
    public TemplateModel wrap(Object obj) throws TemplateModelException {
        if (obj instanceof TemplateModel) {
            return (TemplateModel) obj;
        }
        else if (obj instanceof MessageSpec) {
            return new MessageSpecModel(this, (MessageSpec) obj);
        }
        else if (obj instanceof FieldSpec) {
            return new FieldSpecModel(this, (FieldSpec) obj);
        }
        else if (obj instanceof FieldType) {
            return new FieldTypeModel(this, (FieldType) obj);
        }
        else if (obj instanceof StructSpec) {
            return new StructSpecModel(this, (StructSpec) obj);
        }
        else if (obj instanceof Versions) {
            return new VersionsModel(this, (Versions) obj);
        }
        return super.wrap(obj);
    }

    @Override
    public Object unwrap(TemplateModel tm) throws TemplateModelException {
        if (tm instanceof MessageSpecModel) {
            return ((MessageSpecModel) tm).spec;
        }
        else if (tm instanceof FieldSpecModel) {
            return ((FieldSpecModel) tm).spec;
        }
        else if (tm instanceof StructSpecModel) {
            return ((StructSpecModel) tm).spec;
        }
        else if (tm instanceof FieldTypeModel) {
            return ((FieldTypeModel) tm).fieldType;
        }
        else if (tm instanceof VersionsModel) {
            return ((VersionsModel) tm).versions;
        }
        else {
            return super.unwrap(tm);
        }
    }

    @Override
    public Object tryUnwrapTo(TemplateModel tm, Class<?> targetClass) throws TemplateModelException {
        if (tm instanceof MessageSpecModel
                && targetClass.isInstance(((MessageSpecModel) tm).spec)) {
            return ((MessageSpecModel) tm).spec;
        }
        else if (tm instanceof FieldSpecModel
                && targetClass.isInstance(((FieldSpecModel) tm).spec)) {
            return ((FieldSpecModel) tm).spec;
        }
        else if (tm instanceof StructSpecModel
                && targetClass.isInstance(((StructSpecModel) tm).spec)) {
            return ((StructSpecModel) tm).spec;
        }
        else if (tm instanceof FieldTypeModel
                && targetClass.isInstance(((FieldTypeModel) tm).fieldType)) {
            return ((FieldTypeModel) tm).fieldType;
        }
        else if (tm instanceof VersionsModel
                && targetClass.isInstance(((VersionsModel) tm).versions)) {
            return ((VersionsModel) tm).versions;
        }
        else {
            return super.tryUnwrapTo(tm, targetClass);
        }
    }

}
