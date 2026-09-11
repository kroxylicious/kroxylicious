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

package io.kroxylicious.kafka.message;

/**
 * The request/response {@link MessageSpec} pair registered against a single API key, shared by
 * {@link ApiKeysGenerator} and {@link ApiMessageTypeGenerator}.
 */
class ApiSpec {
    final short apiKey;
    MessageSpec requestSpec;
    MessageSpec responseSpec;

    ApiSpec(short apiKey) {
        this.apiKey = apiKey;
    }

    String name() {
        if (requestSpec != null) {
            return MessageGenerator.stripSuffix(requestSpec.name(), MessageGenerator.REQUEST_SUFFIX);
        }
        else if (responseSpec != null) {
            return MessageGenerator.stripSuffix(responseSpec.name(), MessageGenerator.RESPONSE_SUFFIX);
        }
        else {
            throw new RuntimeException("Neither requestSpec nor responseSpec is defined for API key " + apiKey);
        }
    }
}
