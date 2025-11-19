/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

interface OrderedKey<T> extends Key<T>, Comparable<OrderedKey<T>> {

    String operand();

    @Override
    default int compareTo(OrderedKey<T> o) {
        var cmp = this.type().getName().compareTo(o.type().getName());
        if (cmp == 0) {
            // any < equal < startswith
            if (!this.getClass().equals(o.getClass())) {
                if (this instanceof ResourceMatcherAnyOfType<T>) {
                    return -1;
                }
                else if (this instanceof ResourceMatcherNameStarts<T>) {
                    return 1;
                }
                else if (o instanceof ResourceMatcherAnyOfType<T>) {
                    return 1;
                }
                else if (o instanceof ResourceMatcherNameStarts<T>) {
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
