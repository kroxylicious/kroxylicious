/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;

import javax.net.ssl.TrustManagerFactory;

public interface TrustManagerBuilder {

    void trustCertCollectionFile(File trustStore);

    void trustManager(TrustManagerFactory factory);

    void insecure();

}
