/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels.source.simple;

import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import io.kroxylicious.proxy.labels.Label;
import io.kroxylicious.proxy.labels.LabelledResource;
import io.kroxylicious.proxy.labels.source.LabelSource;
import io.kroxylicious.proxy.labels.source.LabelSourceFactory;
import io.kroxylicious.proxy.labels.source.LabelSourceFactoryContext;
import io.kroxylicious.proxy.labels.source.LabellingContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import static java.util.stream.Collectors.toSet;

@Plugin(configType = MtlsAwareInlineLabelSourceFactory.Config.class)
public class MtlsAwareInlineLabelSourceFactory implements LabelSourceFactory<MtlsAwareInlineLabelSourceFactory.Config, MtlsAwareInlineLabelSourceFactory.Config> {

    @Override
    public Config initialize(LabelSourceFactoryContext context, Config config) {
        return config;
    }

    @Override
    public LabelSource create(Config initializationData) {
        return initializationData;
    }

    public static final class LabelPolicy {
        private final Map<Pattern, Set<Label>> resourceNamePatternToLabels;

        public LabelPolicy(Map<String, Set<Label>> resourceNamePatternToLabels) {
            this.resourceNamePatternToLabels = resourceNamePatternToLabels.entrySet().stream()
                    .collect(Collectors.toMap(e -> Pattern.compile(e.getKey()), Map.Entry::getValue));
        }

        public Set<Label> labels(String resourceName) {
            return resourceNamePatternToLabels
                    .entrySet().stream().filter(e -> e.getKey().matcher(resourceName).matches())
                    .flatMap(e -> e.getValue().stream()).collect(toSet());
        }
    }

    public record ResourceLabels(Map<LabelledResource, LabelPolicy> resourceNameToLabels) {
        public Set<Label> labels(LabelledResource resource, String resourceName) {
            LabelPolicy policy = resourceNameToLabels.getOrDefault(resource, new LabelPolicy(Map.of()));
            return policy.labels(resourceName);
        }
    }

    public record Config(Map<String, ResourceLabels> resourceLabelsPerPrincipal) implements LabelSource {

        @Override
        public Set<Label> labels(LabelledResource resource, String resourceName, LabellingContext context) {
            String principalName = getPrincipalName(context).orElseThrow(() -> new IllegalStateException("could not extract principal name"));
            ResourceLabels labelPolicy = resourceLabelsPerPrincipal.getOrDefault(principalName, new ResourceLabels(Map.of()));
            return labelPolicy.labels(resource, resourceName);
        }

        private Optional<String> getPrincipalName(LabellingContext context) {
            X509Certificate certificate = context.clientTlsContext().flatMap(ClientTlsContext::clientCertificate)
                    .orElseThrow(() -> new IllegalStateException("No client TLS context"));
            String principalName = certificate.getSubjectX500Principal().getName();
            try {
                LdapName name = new LdapName(principalName);
                for (Rdn rdn : name.getRdns()) {
                    if (rdn.getType().equalsIgnoreCase("CN")) {
                        return Optional.of(rdn.getValue().toString());
                    }
                }
            }
            catch (InvalidNameException e) {
                throw new RuntimeException(e);
            }
            return Optional.empty();
        }
    }
}
