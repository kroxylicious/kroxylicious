/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.re2j.Pattern;

import io.kroxylicious.authorizer.provider.acl.allow.FakeClusterResource;
import io.kroxylicious.authorizer.provider.acl.allow.FakeTopicResource;

import static org.assertj.core.api.Assertions.assertThat;

class TypePatternMatchTest {
    @Test
    void match() {
        var m = new TypePatternMatch();
        m.add(new ResourceMatcherNameMatches<>(FakeTopicResource.class, Pattern.compile("baz*")), Set.of(FakeTopicResource.CREATE));
        m.add(new ResourceMatcherNameMatches<>(FakeTopicResource.class, Pattern.compile("baz*")), Set.of(FakeTopicResource.ALTER));
        m.add(new ResourceMatcherNameMatches<>(FakeTopicResource.class, Pattern.compile("bazz")), Set.of(FakeTopicResource.DESCRIBE));
        m.add(new ResourceMatcherNameMatches<>(FakeClusterResource.class, Pattern.compile("foo*")), Set.of(FakeClusterResource.CONNECT));

        assertThat(m.matchingOperations(FakeTopicResource.class, "absent")).isNull();
        assertThat(m.matchingOperations(FakeTopicResource.class, "ba")).isEqualTo(Set.of(FakeTopicResource.CREATE, FakeTopicResource.ALTER));
        assertThat(m.matchingOperations(FakeTopicResource.class, "baz")).isEqualTo(Set.of(FakeTopicResource.CREATE, FakeTopicResource.ALTER));
        assertThat(m.matchingOperations(FakeTopicResource.class, "bazz"))
                .isEqualTo(Set.of(FakeTopicResource.CREATE, FakeTopicResource.ALTER, FakeTopicResource.DESCRIBE));

        assertThat(m.matchingOperations(FakeClusterResource.class, "absent")).isNull();
        assertThat(m.matchingOperations(FakeClusterResource.class, "fo")).isEqualTo(Set.of(FakeClusterResource.CONNECT));
        assertThat(m.matchingOperations(FakeClusterResource.class, "foo")).isEqualTo(Set.of(FakeClusterResource.CONNECT));
        assertThat(m.matchingOperations(FakeClusterResource.class, "fooo")).isEqualTo(Set.of(FakeClusterResource.CONNECT));
    }

}