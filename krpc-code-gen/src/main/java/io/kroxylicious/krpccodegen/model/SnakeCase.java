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

import freemarker.template.SimpleScalar;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;

public class SnakeCase implements TemplateMethodModelEx {
    static String toSnakeCase(String string) {
        StringBuilder bld = new StringBuilder();
        boolean prevWasCapitalized = true;
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isUpperCase(c)) {
                if (!prevWasCapitalized) {
                    bld.append('_');
                }
                bld.append(Character.toLowerCase(c));
                prevWasCapitalized = true;
            }
            else {
                bld.append(c);
                prevWasCapitalized = false;
            }
        }
        return bld.toString();
    }

    @Override
    public Object exec(List arguments) throws TemplateModelException {
        return toSnakeCase(((SimpleScalar) arguments.get(0)).toString());
    }
}
