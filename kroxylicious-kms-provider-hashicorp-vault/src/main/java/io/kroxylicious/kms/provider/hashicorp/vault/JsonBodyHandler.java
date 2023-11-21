/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpResponse;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

class JsonBodyHandler<T> implements HttpResponse.BodyHandler<Supplier<T>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static <T> Supplier<T> toSupplierOfType(InputStream inputStream) {
        return () -> {
            try (InputStream stream = inputStream) {
                return OBJECT_MAPPER.readValue(stream, new TypeReference<T>() {
                });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static <T> HttpResponse.BodySubscriber<Supplier<T>> asJSON() {
        return HttpResponse.BodySubscribers.mapping(
                HttpResponse.BodySubscribers.ofInputStream(),
                JsonBodyHandler::toSupplierOfType);
    }

    @Override
    public HttpResponse.BodySubscriber<Supplier<T>> apply(HttpResponse.ResponseInfo responseInfo) {
        return JsonBodyHandler.asJSON();
    }

}
