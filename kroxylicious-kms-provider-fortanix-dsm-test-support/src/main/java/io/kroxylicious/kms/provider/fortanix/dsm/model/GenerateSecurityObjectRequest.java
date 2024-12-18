/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record GenerateSecurityObjectRequest(@JsonProperty("name") String name,
                                            @JsonProperty("key_size") int keySize,
                                            @JsonProperty("obj_type") String objType,
                                            @JsonProperty("group_id") String groupId) {}
