/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

/**
 * Configuration for the {@link AclAuthorizerService} plugin.
 *
 * @param aclFile the path of the file containing the ACL rules from which the
 *        {@link AclAuthorizer} will be built.
 */
public record AclAuthorizerConfig(
                                  String aclFile) {}
