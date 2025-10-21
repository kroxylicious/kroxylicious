/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.usernamespace;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface ResourceNameMapperService<C> {

    /**
     * Initialises the service.  This method must be invoked exactly once
     * before {@link #build()} is called.
     *
     * @param config KMS service configuration
     */
    void initialize(C config);

    /**
     * Builds a mapper service.
     * {@link #initialize(C)} must have been called before this method is invoked.
     *
     * @return the KMS.
     * @throws IllegalStateException if the mapper service has not been initialised.
     */
    @NonNull
    ResourceNameMapper build() throws IllegalStateException;
}
