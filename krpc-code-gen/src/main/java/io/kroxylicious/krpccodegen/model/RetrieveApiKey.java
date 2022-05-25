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

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;

import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;

/**
 * A custom FreeMarker function which obtains the API key (as an enum name) from a message spec, e.g. "CREATE_TOPICS".
 */
public class RetrieveApiKey implements TemplateMethodModelEx {

    private static String retrieveApiKey(MessageSpecModel messageSpecModel) {
        Short apiKey = messageSpecModel.spec.apiKey().orElseThrow();
        return ApiKeys.forId(apiKey).name();
    }

    @Override
    public Object exec(List arguments) throws TemplateModelException {
        return retrieveApiKey(((MessageSpecModel) arguments.get(0)));
    }
}
