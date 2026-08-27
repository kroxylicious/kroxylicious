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
package io.kroxylicious.kafka.common.protocol;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorsTest {

    @Test
    void shouldHaveUniqueErrorCodes() {
        Set<Short> codeSet = new HashSet<>();
        for (Errors error : Errors.values()) {
            codeSet.add(error.code());
        }
        assertThat(codeSet).hasSize(Errors.values().length);
    }

    @ParameterizedTest
    @EnumSource(Errors.class)
    void shouldHaveAMessage(Errors error) {
        assertThat(error.message()).isNotNull().isNotBlank();
    }

    @ParameterizedTest
    @EnumSource(Errors.class)
    void shouldMapCodeToEnum(Errors error) {
        assertThat(Errors.forCode(error.code())).isEqualTo(error);
    }
}