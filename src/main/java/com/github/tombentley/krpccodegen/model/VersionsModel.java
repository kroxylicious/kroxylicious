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
import com.github.tombentley.krpccodegen.schema.Versions;
import freemarker.template.AdapterTemplateModel;
import freemarker.template.SimpleNumber;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateScalarModel;
import freemarker.template.TemplateSequenceModel;

public class VersionsModel implements TemplateHashModel, TemplateScalarModel, TemplateSequenceModel, AdapterTemplateModel {
    final Versions versions;
    final KrpcSchemaObjectWrapper wrapper;

    public VersionsModel(KrpcSchemaObjectWrapper wrapper, Versions versions) {
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
    public boolean isEmpty() throws TemplateModelException {
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
    public int size() throws TemplateModelException {
        return versions.empty() ? 0 : versions.highest() - versions.lowest() + 1;
    }
}
