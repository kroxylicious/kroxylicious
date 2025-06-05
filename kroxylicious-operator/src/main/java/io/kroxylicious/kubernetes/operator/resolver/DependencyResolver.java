/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.CertificateRef;
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.ProxyRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

/**
 * DependencyResolver resolves the dependencies of a KafkaProxy or VirtualKafkaCluster. We use numerous
 * Custom Resources to model the virtual clusters, ingresses, filters and clusters that will eventually
 * manifest as a single proxy Deployment.
 * <p>
 * DependencyResolver is responsible for resolving all of these references into Custom Resources, returning
 * a result that contains the resolved Custom Resource instances, and a description of any problems encountered.
 * Examples of problems are:
 * <ul>
 * <li>
 *     Dangling References - an entity refers to another entity that cannot be found
 * </li>
 * <li>
 *     ResolvedRefs=False condition - an entity has a condition declaring that its Refs are not resolved
 * </li>
 * </ul>
 */
public class DependencyResolver {

    public static final ProxyResolutionResult EMPTY_RESOLUTION_RESULT = new ProxyResolutionResult(Set.of());

    private DependencyResolver() {
    }

    public static DependencyResolver create() {
        return new DependencyResolver();
    }

    /**
     * Resolves all dependencies of a KafkaProxy recursively (if there are dependencies
     * of dependencies, we resolve them too). Makes the resolved dependencies available and
     * reports any problems during resolution.
     *
     * @param proxy proxy
     * @param context reconciliation context for a KafkaProxy
     * @return a resolution result containing all resolved resources, and a description of resolution problems, if any
     */
    public ProxyResolutionResult resolveProxyRefs(KafkaProxy proxy, Context<?> context) {
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(context);
        Set<VirtualKafkaCluster> virtualKafkaClusters = context.getSecondaryResources(VirtualKafkaCluster.class);
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }

        CommonDependencies commonDependencies = getCommonDependenciesFrom(context);
        var clusterResolutionResults = virtualKafkaClusters.stream()
                .map(cluster -> discoverProblemsAndBuildResolutionResult(cluster, commonDependencies, Set.of(proxy)))
                .collect(Collectors.toSet());
        return new ProxyResolutionResult(clusterResolutionResults);
    }

    private static CommonDependencies getCommonDependenciesFrom(Context<?> context) {
        Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaService>, KafkaService> clusterRefs = context.getSecondaryResources(KafkaService.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters = context.getSecondaryResources(KafkaProtocolFilter.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<ConfigMap>, ConfigMap> configMaps = context.getSecondaryResources(ConfigMap.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<Secret>, Secret> secretes = context.getSecondaryResources(Secret.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        return new CommonDependencies(ingresses, clusterRefs, filters, configMaps, secretes);
    }

    // dependencies common to VirtualKafkaCluster and KafkaProxy reconciliation
    private record CommonDependencies(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                      Map<LocalRef<KafkaService>, KafkaService> kafkaServices,
                                      Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters,
                                      Map<LocalRef<ConfigMap>, ConfigMap> configMaps,
                                      Map<LocalRef<Secret>, Secret> secrets) {
        private CommonDependencies {
            Objects.requireNonNull(ingresses);
            Objects.requireNonNull(kafkaServices);
            Objects.requireNonNull(filters);
            Objects.requireNonNull(configMaps);
            Objects.requireNonNull(secrets);
        }
    }

    /**
     * Resolves all dependencies of a VirtualKafkaCluster recursively (if there are dependencies
     * of dependencies, we resolve them too) and reports any problems during resolution.
     *
     * @param cluster cluster being resolved
     * @param context reconciliation context for a VirtualKafkaCluster
     * @return cluster resolution result containing a description of resolution problems, if any
     */
    public ClusterResolutionResult resolveClusterRefs(VirtualKafkaCluster cluster, Context<?> context) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(context);
        CommonDependencies commonDependencies = getCommonDependenciesFrom(context);
        Set<KafkaProxy> proxies = context.getSecondaryResources(KafkaProxy.class);
        return discoverProblemsAndBuildResolutionResult(cluster, commonDependencies, proxies);
    }

    private ClusterResolutionResult discoverProblemsAndBuildResolutionResult(VirtualKafkaCluster cluster,
                                                                             CommonDependencies commonDependencies,
                                                                             Set<KafkaProxy> proxies) {
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        return new ClusterResolutionResult(cluster,
                resolveProxy(clusterRef, cluster, proxies),
                resolveFilters(clusterRef, cluster, commonDependencies),
                resolveService(clusterRef, cluster, commonDependencies),
                resolveIngresses(clusterRef, cluster, commonDependencies, proxies));
    }

    private ResolutionResult<KafkaProxy> resolveProxy(LocalRef<?> referrer, VirtualKafkaCluster cluster, Set<KafkaProxy> proxies) {
        ProxyRef proxyRef = cluster.getSpec().getProxyRef();
        return resolveProxy(referrer, proxies, proxyRef);
    }

    private static ResolutionResult<KafkaProxy> resolveProxy(LocalRef<?> referrer, Set<KafkaProxy> proxies, ProxyRef proxyRef) {
        return proxies.stream().filter(p -> ResourcesUtil.toLocalRef(p).equals(proxyRef)).findFirst()
                .map(p -> new ResolutionResult<>(referrer, proxyRef, p)).orElse(new ResolutionResult<>(referrer, proxyRef, null));
    }

    private List<ResolutionResult<KafkaProtocolFilter>> resolveFilters(LocalRef<VirtualKafkaCluster> clusterRef,
                                                                       VirtualKafkaCluster cluster,
                                                                       CommonDependencies commonDependencies) {
        return Optional.ofNullable(cluster.getSpec().getFilterRefs()).orElse(List.of()).stream()
                .map(ref -> {
                    Optional<KafkaProtocolFilter> filter = Optional.ofNullable(commonDependencies.filters().get(ref));
                    return new ResolutionResult<>(clusterRef, ref, filter.orElse(null));
                }).toList();
    }

    private ResolutionResult<KafkaService> resolveService(LocalRef<VirtualKafkaCluster> clusterRef,
                                                          VirtualKafkaCluster cluster,
                                                          CommonDependencies commonDependencies) {
        KafkaServiceRef serviceRef = cluster.getSpec().getTargetKafkaServiceRef();
        KafkaService service = commonDependencies.kafkaServices().get(serviceRef);
        Optional<KafkaService> optionalKafkaService = Optional.ofNullable(service);
        return new ResolutionResult<>(clusterRef, serviceRef, optionalKafkaService.orElse(null));
    }

    private List<IngressResolutionResult> resolveIngresses(LocalRef<VirtualKafkaCluster> clusterRef,
                                                           VirtualKafkaCluster cluster,
                                                           CommonDependencies commonDependencies,
                                                           Set<KafkaProxy> proxies) {
        return cluster.getSpec().getIngresses().stream().map(ingress -> {
            IngressRef ingressRef = ingress.getIngressRef();
            KafkaProxyIngress kafkaProxyIngress = commonDependencies.ingresses().get(ingressRef);
            Optional<KafkaProxyIngress> optionalKafkaProxyIngress = Optional.ofNullable(kafkaProxyIngress);
            ResolutionResult<KafkaProxyIngress> resolvedIngress = new ResolutionResult<>(clusterRef, ingressRef,
                    optionalKafkaProxyIngress.orElse(null));
            ResolutionResult<KafkaProxy> kafkaProxyResolutionResult = optionalKafkaProxyIngress
                    .map(i -> resolveProxy(ResourcesUtil.toLocalRef(i), proxies, i.getSpec().getProxyRef()))
                    .orElse(null);
            return new IngressResolutionResult(resolvedIngress, kafkaProxyResolutionResult, ingress,
                    resolveCertificateRefs(clusterRef, ingress.getTls(), commonDependencies));
        }).toList();
    }

    private List<ResolutionResult<? extends HasMetadata>> resolveCertificateRefs(LocalRef<VirtualKafkaCluster> clusterRef,
                                                                                 @Nullable Tls ingressTls,
                                                                                 CommonDependencies commonDependencies) {
        if (ingressTls == null) {
            return List.of();
        }
        CertificateRef certificateRef = ingressTls.getCertificateRef();
        if ("Secret".equals(certificateRef.getKind())) {
            LocalRef<Secret> secretRef = certificateRef.asRefToKind(Secret.class);
            Map<LocalRef<Secret>, Secret> secrets = commonDependencies.secrets();
            return List.of(new ResolutionResult<>(clusterRef, secretRef, secrets.getOrDefault(secretRef, null)));
        }
        else {
            throw new UnsupportedOperationException(
                    "Only CertificateRefs pointing at secrets are supported. Ref:" + certificateRef.getName() + " is pointing at a:" + certificateRef.getKind());
        }
    }
}
