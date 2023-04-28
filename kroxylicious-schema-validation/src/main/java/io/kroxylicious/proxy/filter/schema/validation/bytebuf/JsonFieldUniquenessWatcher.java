/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

class JsonFieldUniquenessWatcher implements JsonObjectWatcher {

    private static final class Node {
        private final Node parent;
        private final String name;
        private final boolean isArray;
        private int index = 0;
        private final Set<String> fields = new HashSet<>();

        private String path() {
            StringBuilder path = new StringBuilder();
            Node node = this;
            while (node != null) {
                path.insert(0, node.name);
                node = node.parent;
            }
            return path.toString();
        }

        private Node(Node parent, String name, boolean isArray) {
            this.parent = parent;
            this.name = name;
            this.isArray = isArray;
        }
    }

    private Node currentNode = null;
    private String currentField = null;

    @Override
    public void onToken(JsonParser parser, JsonToken token) throws IOException {
        switch (token) {
            case START_OBJECT, START_ARRAY -> {
                String name;
                if (currentNode == null) {
                    name = "$";
                }
                else if (currentNode.isArray) {
                    name = "[" + currentNode.index + "]";
                    currentNode.index++;
                }
                else {
                    name = "." + currentField;
                }
                currentNode = new Node(currentNode, name, token == JsonToken.START_ARRAY);
            }
            case END_OBJECT, END_ARRAY -> {
                currentNode = currentNode.parent;
            }
            case FIELD_NAME -> {
                currentField = parser.getCurrentName();
                if (!currentNode.fields.add(currentField)) {
                    throw new IllegalStateException("JSON object at " + currentNode.path() + " contained duplicate key: " + currentField);
                }
            }
            case VALUE_NULL, VALUE_FALSE, VALUE_STRING, VALUE_TRUE, VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT, VALUE_EMBEDDED_OBJECT -> {
                // check null in case root is a value
                if (currentNode != null && currentNode.isArray) {
                    currentNode.index++;
                }
            }
            default -> {

            }
        }
    }
}
