/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.Objects;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A BodyHandler that deserializes JSON data using a Jackson ObjectMapper yielding an object of
 * type {@code T}.
 * @param <T> result type.
 */
class JsonBodyHandler<T> implements BodyHandler<Supplier<T>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @NonNull
    private final TypeReference<T> typeRef;

    JsonBodyHandler(@NonNull TypeReference<T> typeRef) {
        Objects.requireNonNull(typeRef);
        this.typeRef = typeRef;
    }

    @Override
    public HttpResponse.BodySubscriber<Supplier<T>> apply(HttpResponse.ResponseInfo responseInfo) {
        return HttpResponse.BodySubscribers.mapping(
                HttpResponse.BodySubscribers.ofInputStream(),
                inputStream -> () -> {
                    try (var stream = inputStream) {
                        return OBJECT_MAPPER.readValue(stream, typeRef);

                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }
}
