/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.kroxylicious.proxy.audit.Actor;
import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;
import io.kroxylicious.proxy.audit.ClientActor;
import io.kroxylicious.proxy.audit.Correlation;
import io.kroxylicious.proxy.audit.ProxyActor;
import io.kroxylicious.proxy.audit.ServerActor;
import io.kroxylicious.proxy.authentication.Principal;

class AuditEmitterContextImpl implements AuditEmitter.Context {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Override
    public String asString(AuditableAction action, AuditEmitter.TextFormat format) {
        if (format == AuditEmitter.TextFormat.KROXYLICIOUS_JSON_V1) {
            try {
                StringWriter w = new StringWriter();
                JsonGenerator generator = JSON_FACTORY.createGenerator(w);
                writeAction(action, generator);
                generator.flush();
                generator.close();
                return w.toString();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else {
            throw new IllegalArgumentException("Unknown format: " + format);
        }
    }

    @Override
    public byte[] asBytes(AuditableAction action, AuditEmitter.BinaryFormat format) {
        if (format == AuditEmitter.BinaryFormat.KROXYLICIOUS_JSON_V1) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                JsonGenerator generator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8);
                writeAction(action, generator);
                generator.flush();
                generator.close();
                return out.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else {
            throw new IllegalArgumentException("Unknown format: " + format);
        }
    }

    private void writeAction(AuditableAction action, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName("time");
        generator.writeString(action.time().toString());
        generator.writeFieldName("action");
        generator.writeString(action.action());
        generator.writeFieldName("status");
        generator.writeString(action.status());
        generator.writeFieldName("reason");
        generator.writeString(action.reason());
        generator.writeFieldName("actor");
        writeActor(generator, action.actor());
        generator.writeFieldName("objectRef");
        writeMapOfStrings(generator, action.objectRef());
        writeCorrelation(generator, action.correlation());
        Map<String, String> context = action.context();
        if (context != null) {
            generator.writeFieldName("context");
            writeMapOfStrings(generator, context);
        }
        generator.writeEndObject(); // action
    }

    private static void writeCorrelation(JsonGenerator generator, Correlation correlation) throws IOException {
        var clientCorrelationId = correlation.clientCorrelationId();
        var serverCorrelationId = correlation.serverCorrelationId();
        if (clientCorrelationId != null || serverCorrelationId != null) {
            generator.writeFieldName("correlation");
            generator.writeStartObject();
            if (clientCorrelationId != null) {
                generator.writeFieldName("client");
                generator.writeNumber(clientCorrelationId);
            }
            if (serverCorrelationId != null) {
                generator.writeFieldName("server");
                generator.writeNumber(serverCorrelationId);
            }
            generator.writeEndObject();
        }
    }

    private static void writeMapOfStrings(JsonGenerator generator,
                                          Map<String, String> stringStringMap)
            throws IOException {
        generator.writeStartObject();
        for (var entry : stringStringMap.entrySet()) {
            String scope = entry.getKey();
            String identifier = entry.getValue();
            generator.writeFieldName(scope);
            generator.writeString(identifier);
        }
        generator.writeEndObject();
    }

    private static void writeActor(JsonGenerator generator, Actor actor) throws IOException {
        generator.writeStartObject();
        if (actor instanceof ClientActor clientActor) {
            generator.writeFieldName("srcAddr");
            generator.writeString(clientActor.srcAddr().toString());
            generator.writeFieldName("session");
            generator.writeString(clientActor.session());
            var principals = clientActor.principals();
            if (principals != null) {
                generator.writeFieldName("principals");
                generator.writeStartArray();
                for (Principal principal : principals) {
                    writePrincipal(generator, principal);
                }
                generator.writeEndArray();
            }
        }
        else if (actor instanceof ServerActor serverActor) {
            generator.writeFieldName("tgtAddr");
            generator.writeString(serverActor.tgtAddr().toString());
            generator.writeFieldName("hostname");
            generator.writeString(serverActor.hostname());
            Integer nodeId = serverActor.nodeId();
            if (nodeId != null) {
                generator.writeFieldName("nodeId");
                generator.writeNumber(nodeId);
            }
        }
        else if (actor instanceof ProxyActor selfActor) {
            generator.writeFieldName("executionId");
            generator.writeString(selfActor.executionId());
        }
        generator.writeEndObject();
    }

    private static void writePrincipal(JsonGenerator generator, Principal principal) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName("type");
        generator.writeString(principal.getClass().getName());
        generator.writeFieldName("name");
        generator.writeString(principal.name());
        generator.writeEndObject();
    }
}
