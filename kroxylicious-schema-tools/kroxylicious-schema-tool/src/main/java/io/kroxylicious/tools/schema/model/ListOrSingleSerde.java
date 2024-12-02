/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class ListOrSingleSerde {

    public ListOrSingleSerde() {
    }

    public abstract static class ListOrSingleDeserializer<T> extends JsonDeserializer<List<T>> {
        private final TypeReference<T> elementTypeReference;
        private final TypeReference<List<T>> listTypeReference;

        protected ListOrSingleDeserializer(
                                           TypeReference<T> elementTypeReference) {
            this.elementTypeReference = elementTypeReference;
            this.listTypeReference = new TypeReference<List<T>>() {
                @Override
                public java.lang.reflect.Type getType() {
                    TypeFactory typeFactory = TypeFactory.defaultInstance();
                    JavaType javaType = typeFactory.constructType(elementTypeReference);
                    return typeFactory.constructCollectionType(List.class, javaType);
                }
            };
        }

        @Override
        public List<T> deserialize(
                                   JsonParser parser,
                                   DeserializationContext context)
                throws IOException {
            if (parser.isExpectedStartArrayToken()) {
                return parser.readValueAs(listTypeReference);
            }
            else {
                var list = new ArrayList<T>();
                list.add(parser.readValueAs(elementTypeReference));
                return list;
            }
        }
    }

    public static class String extends ListOrSingleDeserializer<java.lang.String> {
        public String() {
            super(new TypeReference<java.lang.String>() {
            });
        }
    }

    public static class SchemaType extends ListOrSingleDeserializer<io.kroxylicious.tools.schema.model.SchemaType> {
        public SchemaType() {
            super(new TypeReference<io.kroxylicious.tools.schema.model.SchemaType>() {
            });
        }
    }

    public static class SchemaObject extends ListOrSingleDeserializer<io.kroxylicious.tools.schema.model.SchemaObject> {
        public SchemaObject() {
            super(new TypeReference<io.kroxylicious.tools.schema.model.SchemaObject>() {
            });
        }
    }

}
