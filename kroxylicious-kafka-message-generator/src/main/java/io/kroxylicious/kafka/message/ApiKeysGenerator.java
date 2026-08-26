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
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generates the Kroxylicious-owned {@code ApiKeys} enum as a thin veneer over the generated
 * {@code ApiMessageType}. Everything it exposes is spec-derivable and delegates to
 * {@code ApiMessageType}; the broker/controller-internal {@code clusterAction} and
 * {@code forwardable} flags (and members with no proxy consumer, such as the listener
 * groupings) are intentionally omitted. See issue #4752.
 */
public final class ApiKeysGenerator implements TypeClassGenerator {

    // The fixed body emitted after the generated enum constants. Kept as verbatim source lines
    // (already indented for the class body) so nothing here is spec-derived except the constants.
    private static final String[] BODY = {
            "",
            "    // Versions 0-2 were removed in Apache Kafka 4.0, version 3 is the new baseline. Due to a bug in",
            "    // librdkafka, version 0 has to be included in the api versions response (see KAFKA-18659).",
            "    public static final short PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION = 0;",
            "",
            "    /** the permanent and immutable id of an API - this can't change ever */",
            "    public final short id;",
            "",
            "    /** An english description of the api - used for debugging and metric names */",
            "    public final String name;",
            "",
            "    public final ApiMessageType messageType;",
            "",
            "    private static final Map<Integer, ApiKeys> ID_TO_TYPE = new HashMap<>();",
            "",
            "    static {",
            "        for (ApiKeys apiKey : values()) {",
            "            ID_TO_TYPE.put((int) apiKey.id, apiKey);",
            "        }",
            "    }",
            "",
            "    ApiKeys(ApiMessageType messageType) {",
            "        this.messageType = messageType;",
            "        this.id = messageType.apiKey();",
            "        this.name = messageType.name;",
            "    }",
            "",
            "    public static ApiKeys forId(int id) {",
            "        ApiKeys apiKey = ID_TO_TYPE.get(id);",
            "        if (apiKey == null) {",
            "            throw new IllegalArgumentException(\"Unexpected api key: \" + id);",
            "        }",
            "        return apiKey;",
            "    }",
            "",
            "    public static boolean hasId(int id) {",
            "        return ID_TO_TYPE.containsKey(id);",
            "    }",
            "",
            "    public short latestVersion() {",
            "        return messageType.highestSupportedVersion(true);",
            "    }",
            "",
            "    public short latestVersion(boolean enableUnstableLastVersion) {",
            "        return messageType.highestSupportedVersion(enableUnstableLastVersion);",
            "    }",
            "",
            "    public short oldestVersion() {",
            "        return messageType.lowestSupportedVersion();",
            "    }",
            "",
            "    public List<Short> allVersions() {",
            "        List<Short> versions = new ArrayList<>(latestVersion() - oldestVersion() + 1);",
            "        for (short version = oldestVersion(); version <= latestVersion(); version++) {",
            "            versions.add(version);",
            "        }",
            "        return versions;",
            "    }",
            "",
            "    public boolean isVersionSupported(short apiVersion) {",
            "        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();",
            "    }",
            "",
            "    /**",
            "     * Returns {@code true} if there is at least one valid version. When {@code false}, the api key",
            "     * remains assigned to a removed api so it is not accidentally reused for a different api.",
            "     */",
            "    public boolean hasValidVersion() {",
            "        return oldestVersion() <= latestVersion();",
            "    }",
            "",
            "    public short requestHeaderVersion(short apiVersion) {",
            "        return messageType.requestHeaderVersion(apiVersion);",
            "    }",
            "",
            "    public short responseHeaderVersion(short apiVersion) {",
            "        return messageType.responseHeaderVersion(apiVersion);",
            "    }",
            "}"
    };

    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, ApiData> apis;

    private static final class ApiData {
        final short apiKey;
        MessageSpec requestSpec;
        MessageSpec responseSpec;

        ApiData(short apiKey) {
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

        String constantName() {
            return MessageGenerator.toSnakeCase(name()).toUpperCase(Locale.ROOT);
        }
    }

    public ApiKeysGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.apis = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName() {
        return MessageGenerator.API_KEYS_JAVA;
    }

    @Override
    public void registerMessageType(MessageSpec spec) {
        switch (spec.type()) {
            case REQUEST: {
                short apiKey = spec.apiKey().orElseThrow();
                ApiData data = apis.computeIfAbsent(apiKey, ApiData::new);
                if (data.requestSpec != null) {
                    throw new RuntimeException("Found more than one request with API key " + apiKey);
                }
                data.requestSpec = spec;
                break;
            }
            case RESPONSE: {
                short apiKey = spec.apiKey().orElseThrow();
                ApiData data = apis.computeIfAbsent(apiKey, ApiData::new);
                if (data.responseSpec != null) {
                    throw new RuntimeException("Found more than one response with API key " + apiKey);
                }
                data.responseSpec = spec;
                break;
            }
            default:
                // do nothing
                break;
        }
    }

    @Override
    public void generateAndWrite(BufferedWriter writer) throws IOException {
        generate();
        write(writer);
    }

    private void generate() {
        headerGenerator.addImport("io.kroxylicious.kafka.common.message.ApiMessageType");
        headerGenerator.addImport("java.util.ArrayList");
        headerGenerator.addImport("java.util.HashMap");
        headerGenerator.addImport("java.util.List");
        headerGenerator.addImport("java.util.Map");

        buffer.printf("%s%n", "public enum ApiKeys {");
        int numProcessed = 0;
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            numProcessed++;
            String constant = entry.getValue().constantName();
            String terminator = (numProcessed == apis.size()) ? ";" : ",";
            buffer.printf("%s%n", "    " + constant + "(ApiMessageType." + constant + ")" + terminator);
        }
        for (String line : BODY) {
            buffer.printf("%s%n", line);
        }
        headerGenerator.generate();
    }

    private void write(BufferedWriter writer) throws IOException {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }
}
