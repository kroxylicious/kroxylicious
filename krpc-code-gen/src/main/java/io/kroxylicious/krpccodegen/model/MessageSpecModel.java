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
import io.kroxylicious.krpccodegen.schema.MessageSpec;

public class MessageSpecModel implements TemplateHashModel, AdapterTemplateModel {
    final MessageSpec spec;
    final KrpcSchemaObjectWrapper wrapper;

    public MessageSpecModel(KrpcSchemaObjectWrapper wrapper, MessageSpec spec) {
        this.wrapper = wrapper;
        this.spec = spec;
    }

    @Override
    public TemplateModel get(String key) throws TemplateModelException {
        switch (key) {
            case "struct":
                return wrapper.wrap(spec.struct());
            case "name":
                return wrapper.wrap(spec.name());
            case "validVersions":
                return wrapper.wrap(spec.validVersions());
            case "validVersionsString":
                return wrapper.wrap(spec.validVersionsString());
            case "fields":
                return wrapper.wrap(spec.fields());
            case "commonStructs":
                return wrapper.wrap(spec.commonStructs());
            case "flexibleVersions":
                return wrapper.wrap(spec.flexibleVersions());
            case "flexibleVersionsString":
                return wrapper.wrap(spec.flexibleVersionsString());
            case "apiKey":
                return wrapper.wrap(spec.apiKey());
            case "type":
                return wrapper.wrap(spec.type());
            case "listeners":
                return wrapper.wrap(spec.listeners());
            case "dataClassName":
                return wrapper.wrap(spec.dataClassName());

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
