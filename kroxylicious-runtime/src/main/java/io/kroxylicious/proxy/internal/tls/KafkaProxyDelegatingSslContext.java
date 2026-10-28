/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.DelegatingSslContext;
import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.internal.util.StableKroxyliciousLinkGenerator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A Netty SslContext can be created but not used immediately and this causes an issue when the context needs to exert control
 * when it is used to create an SSL Engine. This class allows configuration to be applied to every SSL Engine
 * created from the delegate. Initially this supports setting the named groups but can be enhanced in the future to control
 * other SSL parameters.
 */
public class KafkaProxyDelegatingSslContext extends DelegatingSslContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyDelegatingSslContext.class);

    private final String id;
    @Nullable
    private volatile AllowDeny<String> namedGroupConfig = null;

    /**
     * Create a new delegating SSL context.
     * @param ctx context to delegate to
     * @param id unique id for this delegate (primarily used in logging)
     */
    public KafkaProxyDelegatingSslContext(final SslContext ctx, final String id) {
        super(ctx);
        this.id = id;
    }

    /**
     * Apply the named groups from the Tls configuration to any connections made through this context.
     * @param tls tls configuration to use
     * @return this
     */
    public KafkaProxyDelegatingSslContext setNamedGroups(final Tls tls) {
        namedGroupConfig = tls.namedGroups();
        return this;
    }

    @Override
    protected void initEngine(final SSLEngine engine) {
        final SSLParameters params = engine.getSSLParameters();

        Optional.ofNullable(namedGroupConfig).ifPresent(namedGroups -> {
            if (Objects.nonNull(namedGroups.allowed()) || Objects.nonNull(namedGroups.denied())) {
                // resolve named groups if either an allowed or denied list is configured, otherwise the platform defaults will be used
                final List<String> resolvedGroups = resolveNamedGroups(params, namedGroups);
                LOGGER.atDebug()
                        .addKeyValue("id", id)
                        .addKeyValue("namedGroups", resolvedGroups)
                        .log("applying named groups to SSL engine");
                if (resolvedGroups.isEmpty()) {
                    LOGGER.atWarn()
                            .addKeyValue("id", id)
                            .log("All allowed named groups are denied, clients will be unable to connect to this endpoint");
                }
                params.setNamedGroups(resolvedGroups.toArray(String[]::new));
            }
        });

        engine.setSSLParameters(params);
    }

    /**
     * Resolves the configured named groups into the concrete, ordered list to apply to each SSL engine.
     * When only a denied list is given, the JDK's default named groups are pruned. Those defaults are only available
     * on a JDK that exposes them, so a denied-only configuration is rejected defensively on a non-compliant JDK rather
     * than silently leaving the denied groups enabled. This is normally caught earlier by {@link Tls#validateNamedGroups}.
     *
     * @param params the SSL parameters supplying the JDK default named groups when only a denied list is configured.
     * @param namedGroups the configured allow/deny lists.
     * @return the ordered named groups to apply.
     */
    private List<String> resolveNamedGroups(final SSLParameters params, final AllowDeny<String> namedGroups) {
        final var hasAllowed = Objects.nonNull(namedGroups.allowed()) && !namedGroups.allowed().isEmpty();
        final var hasDenied = Objects.nonNull(namedGroups.denied()) && !namedGroups.denied().isEmpty();
        final List<String> resolvedGroups;
        if (hasAllowed) {
            resolvedGroups = new ArrayList<>(namedGroups.allowed());
        }
        else {
            var defaults = params.getNamedGroups();
            if (defaults == null) {
                throw new IllegalConfigurationException(
                        ("%s: 'namedGroups' specifies only a 'denied' list, but this JDK does not expose its default named groups, "
                                + "so the denied groups cannot be applied. Specify an 'allowed' list instead. See %s for details.")
                                .formatted(id,
                                        StableKroxyliciousLinkGenerator.INSTANCE.errorLink(StableKroxyliciousLinkGenerator.CLIENT_TLS)));
            }
            resolvedGroups = new ArrayList<>(Arrays.asList(defaults));
        }

        if (hasDenied) {
            // remove any named groups in the denied list (case-insensitively, as the JDK reports mixed casing)
            namedGroups.denied().forEach(denied -> resolvedGroups.removeIf(group -> group.equalsIgnoreCase(denied)));
        }

        return resolvedGroups;
    }

}
