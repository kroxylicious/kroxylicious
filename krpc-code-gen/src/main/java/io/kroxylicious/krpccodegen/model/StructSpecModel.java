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

import freemarker.template.AdapterTemplateModel;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import io.kroxylicious.krpccodegen.schema.StructSpec;

public class StructSpecModel implements TemplateHashModel, AdapterTemplateModel {
    final StructSpec spec;
    final KrpcSchemaObjectWrapper wrapper;

    public StructSpecModel(KrpcSchemaObjectWrapper wrapper, StructSpec ms) {
        this.wrapper = wrapper;
        this.spec = ms;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        switch (key) {
            case "name":
                return wrapper.wrap(spec.name());
            case "fields":
                return wrapper.wrap(spec.fields());
            case "versions":
                return wrapper.wrap(spec.versions());
            case "versionsString":
                return wrapper.wrap(spec.versionsString());
            case "hasKeys":
                return wrapper.wrap(spec.hasKeys());
        }
        throw new TemplateModelException(spec.getClass().getSimpleName() + " doesn't have property " + key);
    }

    @Override
    public boolean isEmpty() throws TemplateModelException {
        return false;
    }

    @Override
    public Object getAdaptedObject(Class<?> hint) {
        return spec;
    }
}
