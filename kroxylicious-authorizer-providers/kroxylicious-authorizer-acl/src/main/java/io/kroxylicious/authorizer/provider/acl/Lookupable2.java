/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

interface Lookupable2<T> extends Lookupable<T>, Comparable<Lookupable2<T>> {

    @Override
    default int compareTo(Lookupable2<T> o) {
        var cmp = this.type().getName().compareTo(o.type().getName());
        if (cmp == 0) {
            // any < equal < startswith
            if (!this.getClass().equals(o.getClass())) {
                if (this instanceof AclAuthorizer.ResourceMatcherAnyOfType<T>) {
                    return -1;
                }
                else if (this instanceof AclAuthorizer.ResourceMatcherNameStarts<T>) {
                    return 1;
                }
                else if (o instanceof AclAuthorizer.ResourceMatcherAnyOfType<T>) {
                    return 1;
                }
                else if (o instanceof AclAuthorizer.ResourceMatcherNameStarts<T>) {
                    return -1;
                }
            }
        }
        if (cmp == 0) {
            if (this.operand() == null) {
                cmp = o.operand() == null ? 0 : -1;
            }
            else if (o.operand() == null) {
                cmp = 1;
            }
            else {
                cmp = this.operand().compareTo(o.operand());
            }
        }
        return cmp;
    }
}
