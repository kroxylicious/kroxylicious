/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.addressmapper;

import io.kroxylicious.proxy.service.Contributor;

/**
 * AddressManagerContributor acts source of {@link AddressManager} implementations.
 * Users wishing to provide alternative AddressManager must implement this class and register the service on the
 * classpath. See {@link java.util.ServiceLoader} for details.
 */
public interface AddressManagerContributor extends Contributor<AddressManager> {

}
