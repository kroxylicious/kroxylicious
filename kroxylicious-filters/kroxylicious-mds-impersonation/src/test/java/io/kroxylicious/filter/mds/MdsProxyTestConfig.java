/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.file.Path;

final class MdsProxyTestConfig {
    private MdsProxyTestConfig() {
    }

    static String configuration(Path directory, int port, TestMdsServer mds) {
        return """
                clusterDefinitions:
                  - name: upstream
                    bootstrapServers: localhost:%d
                    tls:
                      trust:
                        storeFile: %s/mds.crt
                        storeType: PEM
                filterDefinitions:
                  - name: mds
                    type: MdsImpersonation
                    config:
                      mdsUrl: %s
                      mdsTls:
                        key:
                          privateKeyFile: %s/proxy.key
                          certificateFile: %s/proxy.crt
                        trust:
                          storeFile: %s/mds.crt
                          storeType: PEM
                virtualClusters:
                  - name: confluent
                    target:
                      cluster: upstream
                    filters: [mds]
                    subjectBuilder:
                      type: DefaultTransportSubjectBuilderService
                      config:
                        addPrincipals:
                          - from: clientTlsSubject
                            map:
                              - replaceMatch: '#^CN=localhost$#alice#'
                              - else: anonymous
                            principalFactory: io.kroxylicious.proxy.authentication.UserFactory
                    gateways:
                      - name: clients
                        sniHostIdentifiesNode:
                          bootstrapAddress: localhost:0
                          advertisedBrokerAddressPattern: broker-$(nodeId).localhost:0
                        tls:
                          key:
                            privateKeyFile: %s/mds.key
                            certificateFile: %s/mds.crt
                          trust:
                            storeFile: %s/client.crt
                            storeType: PEM
                            trustOptions:
                              clientAuth: REQUIRED
                """.formatted(port, directory, mds.config(mds.clientTls).mdsUrl(), directory, directory, directory, directory, directory, directory);
    }
}
