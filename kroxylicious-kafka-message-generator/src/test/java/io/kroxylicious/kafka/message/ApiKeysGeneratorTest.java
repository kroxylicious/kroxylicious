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

import java.io.BufferedWriter;
import java.io.StringWriter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiKeysGeneratorTest {

    private static final String PACKAGE = "io.kroxylicious.kafka.common.protocol";

    private static MessageSpec requestSpec(int apiKey, String name) throws Exception {
        return spec(apiKey, "request", name + "Request");
    }

    private static MessageSpec responseSpec(int apiKey, String name) throws Exception {
        return spec(apiKey, "response", name + "Response");
    }

    private static MessageSpec spec(int apiKey, String type, String name) throws Exception {
        return MessageGenerator.JSON_SERDE.readValue(String.join("\n",
                "{",
                "  \"apiKey\": " + apiKey + ",",
                "  \"type\": \"" + type + "\",",
                "  \"name\": \"" + name + "\",",
                "  \"validVersions\": \"none\"",
                "}"), MessageSpec.class);
    }

    private static String generate(MessageSpec... specs) throws Exception {
        var generator = new ApiKeysGenerator(PACKAGE);
        for (var spec : specs) {
            generator.registerMessageType(spec);
        }
        var writer = new StringWriter();
        var bufferedWriter = new BufferedWriter(writer);
        generator.generateAndWrite(bufferedWriter);
        bufferedWriter.flush();
        return writer.toString();
    }

    @Test
    void outputName() {
        // Given
        var generator = new ApiKeysGenerator(PACKAGE);

        // When
        var outputName = generator.outputName();

        // Then
        assertEquals("ApiKeys.java", outputName);
    }

    @Test
    void generatesEnumConstantVeneeringApiMessageType() throws Exception {
        // Given
        var request = requestSpec(0, "FooBar");
        var response = responseSpec(0, "FooBar");

        // When
        var generated = generate(request, response);

        // Then
        assertTrue(generated.contains("package " + PACKAGE + ";"), generated);
        assertTrue(generated.contains("public enum ApiKeys {"), generated);
        assertTrue(generated.contains("FOO_BAR(ApiMessageType.FOO_BAR);"), generated);
    }

    @Test
    void ordersConstantsByApiKey() throws Exception {
        // Given
        var second = requestSpec(1, "Second");
        var first = requestSpec(0, "First");

        // When
        var generated = generate(second, first);

        // Then
        var firstIndex = generated.indexOf("FIRST(ApiMessageType.FIRST),");
        var secondIndex = generated.indexOf("SECOND(ApiMessageType.SECOND);");
        assertTrue(firstIndex >= 0, generated);
        assertTrue(secondIndex > firstIndex, generated);
    }

    @Test
    void rejectsDuplicateRequestForSameApiKey() throws Exception {
        // Given
        var generator = new ApiKeysGenerator(PACKAGE);
        generator.registerMessageType(requestSpec(0, "FooBar"));
        var spec = requestSpec(0, "FooBarAgain");

        // When / Then
        var exception = assertThrows(RuntimeException.class,
                () -> generator.registerMessageType(spec));
        assertTrue(exception.getMessage().contains("more than one request with API key 0"), exception.getMessage());
    }
}
