/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.substitution.lookup;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public interface InterpolatorLookupFactory {

    String getType();

    StringLookup create();

    static StringLookup interpolatorStringLookup() {

        List<ServiceLoader.Provider<InterpolatorLookupFactory>> x = ServiceLoader.load(InterpolatorLookupFactory.class).stream().toList();

        // check for duplicate keys.

        var map = x.stream()
                .map(ServiceLoader.Provider::get)
                .collect(Collectors.toMap(InterpolatorLookupFactory::getType,
                        InterpolatorLookupFactory::create));

        return new InterpolatorStringLookup(map);

    }

}
