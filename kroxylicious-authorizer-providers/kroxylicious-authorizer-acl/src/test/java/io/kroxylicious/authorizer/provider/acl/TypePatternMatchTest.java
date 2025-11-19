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
        m.compute(FakeTopicResource.class, Pattern.compile("baz*"), Set.of(FakeTopicResource.CREATE));
        m.compute(FakeTopicResource.class, Pattern.compile("baz*"), Set.of(FakeTopicResource.ALTER));
        m.compute(FakeClusterResource.class, Pattern.compile("foo*"), Set.of(FakeClusterResource.CONNECT));

        assertThat(m.lookup(FakeTopicResource.class, "absent")).isNull();
        assertThat(m.lookup(FakeTopicResource.class, "ba")).isEqualTo(Set.of(FakeTopicResource.CREATE, FakeTopicResource.ALTER));
        assertThat(m.lookup(FakeTopicResource.class, "baz")).isEqualTo(Set.of(FakeTopicResource.CREATE, FakeTopicResource.ALTER));
        assertThat(m.lookup(FakeTopicResource.class, "bazz")).isEqualTo(Set.of(FakeTopicResource.CREATE, FakeTopicResource.ALTER));

        assertThat(m.lookup(FakeClusterResource.class, "absent")).isNull();
        assertThat(m.lookup(FakeClusterResource.class, "fo")).isEqualTo(Set.of(FakeClusterResource.CONNECT));
        assertThat(m.lookup(FakeClusterResource.class, "foo")).isEqualTo(Set.of(FakeClusterResource.CONNECT));
        assertThat(m.lookup(FakeClusterResource.class, "fooo")).isEqualTo(Set.of(FakeClusterResource.CONNECT));
    }

}