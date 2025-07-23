/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.security.auth.x500.X500Principal;

import org.apache.kafka.common.header.internals.RecordHeader;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.proxy.tls.ServerTlsContext;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A filter that adds {@linkplain FilterContext#clientTlsContext() client-facing TLS context}-dependent headers to produced records.
 * Tests can consume the produced records and assert that those records have the expected header values.
 */
public class ClientAuthAwareLawyerFilter
        extends AbstractProduceHeaderInjectionFilter {

    @NonNull
    private static String headerName(String hashtag) {
        return ClientAuthAwareLawyerFilter.class.getSimpleName() + hashtag;
    }

    public static final String HEADER_KEY_CLIENT_TLS = headerName("#clientTlsContext.isPresent");
    public static final String HEADER_KEY_CLIENT_TLS_PROXY_X500PRINCIPAL_NAME = headerName("#clientTlsContext.proxyServerCertificate.principalName");
    public static final String HEADER_KEY_CLIENT_TLS_CLIENT_X500PRINCIPAL_NAME = headerName("#clientTlsContext.clientCertificate.principalName");

    public static final String HEADER_KEY_SERVER_TLS = headerName("#serverTlsContext.isPresent");
    public static final String HEADER_KEY_SERVER_TLS_PROXY_X500PRINCIPAL_NAME = headerName("#serverTlsContext.serverCertificate.principalName");
    public static final String HEADER_KEY_SERVER_TLS_SERVER_X500PRINCIPAL_NAME = headerName("#serverTlsContext.proxyClientCertificate.principalName");

    public static final String HEADER_KEY_CLIENT_SASL_CLIENT_SASLPRINCIPAL_NAME = headerName("#clientSaslContext.clientPrincipal");
    public static final String HEADER_KEY_CLIENT_SASL_MECH_NAME = headerName("#clientSaslContext.mechanismName");
    public static final String HEADER_KEY_CLIENT_SASL_PROXY_SASLPRINCIPAL_NAME = headerName("#clientSaslContext.proxyServerPrincipal");

    private static final Map<String, Function<FilterContext, byte[]>> HEADERS = Map.of(
            HEADER_KEY_CLIENT_TLS,
            context -> context.clientTlsContext().isPresent() ? new byte[]{ 1 } : new byte[]{ 0 },
            HEADER_KEY_CLIENT_TLS_PROXY_X500PRINCIPAL_NAME,
            context -> context.clientTlsContext()
                    .map(ClientTlsContext::proxyServerCertificate)
                    .map(ClientAuthAwareLawyerFilter::principalName)
                    .map(String::getBytes)
                    .orElse(null),
            HEADER_KEY_CLIENT_TLS_CLIENT_X500PRINCIPAL_NAME,
            context -> context.clientTlsContext()
                    .map(ClientTlsContext::clientCertificate)
                    .flatMap(opt -> opt.map(ClientAuthAwareLawyerFilter::principalName))
                    .map(String::getBytes)
                    .orElse(null),

            HEADER_KEY_SERVER_TLS,
            context -> context.serverTlsContext().isPresent() ? new byte[]{ 1 } : new byte[]{ 0 },
            HEADER_KEY_SERVER_TLS_PROXY_X500PRINCIPAL_NAME,
            context -> context.serverTlsContext()
                    .map(ServerTlsContext::proxyClientCertificate)
                    .flatMap(opt -> opt.map(ClientAuthAwareLawyerFilter::principalName))
                    .map(String::getBytes)
                    .orElse(null),
            HEADER_KEY_SERVER_TLS_SERVER_X500PRINCIPAL_NAME,
            context -> context.serverTlsContext()
                    .map(ServerTlsContext::serverCertificate)
                    .map(ClientAuthAwareLawyerFilter::principalName)
                    .map(String::getBytes)
                    .orElse(null),

            HEADER_KEY_CLIENT_SASL_CLIENT_SASLPRINCIPAL_NAME,
            context -> context.clientSaslContext()
                    .map(ClientSaslContext::authorizationId)
                    .map(String::getBytes)
                    .orElse(null),
            HEADER_KEY_CLIENT_SASL_MECH_NAME,
            context -> context.clientSaslContext()
                    .map(ClientSaslContext::mechanismName)
                    .map(String::getBytes)
                    .orElse(null),
            HEADER_KEY_CLIENT_SASL_PROXY_SASLPRINCIPAL_NAME,
            context -> context.clientSaslContext()
                    .flatMap(ClientSaslContext::proxyServerId)
                    .map(String::getBytes)
                    .orElse(null)

    );

    private static String principalName(X509Certificate x509Certificate) {
        return x509Certificate.getSubjectX500Principal()
                .getName(X500Principal.RFC1779,
                        Map.of("1.2.840.113549.1.9.1", "emailAddress"));
    }

    @NonNull
    @Override
    protected List<RecordHeader> headersToAdd(FilterContext context) {
        var headers = new ArrayList<RecordHeader>();
        for (var entry : HEADERS.entrySet()) {
            headers.add(new RecordHeader(entry.getKey(), entry.getValue().apply(context)));
        }
        return headers;
    }

}
