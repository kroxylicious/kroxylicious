/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AbstractResolverTest {

    enum MyEnum {
        FOO,
        BAR,
        QUUX
    }

    interface MyInterface extends PersistedIdentifiable<MyEnum> {

    }

    static class MyImpl implements MyInterface {

        private final byte id;
        private final MyEnum name;

        MyImpl(
                int id,
                MyEnum name
        ) {
            this.id = (byte) id;
            this.name = name;
        }

        @Override
        public byte serializedId() {
            return id;
        }

        @Override
        public MyEnum name() {
            return name;
        }
    }

    static class MyResolver extends AbstractResolver<MyEnum, MyInterface, MyResolver> {

        protected MyResolver(Collection<MyInterface> impls) {
            super(impls);
        }

        @Override
        protected MyResolver newInstance(Collection<MyInterface> values) {
            return new MyResolver(values);
        }
    }

    @Test
    void fromName() {
        MyImpl foo = new MyImpl(0, MyEnum.FOO);
        MyImpl bar = new MyImpl(1, MyEnum.BAR);
        var resolver = new MyResolver(List.of(foo, bar));
        assertThat(resolver.fromName(MyEnum.FOO)).isSameAs(foo);
        assertThat(resolver.fromName(MyEnum.BAR)).isSameAs(bar);

        assertThatThrownBy(() -> resolver.fromName(MyEnum.QUUX))
                                                                .isExactlyInstanceOf(EncryptionException.class)
                                                                .hasMessage("Unknown MyEnum name: QUUX");
    }

    @Test
    void fromSerializedId() {
        MyImpl foo = new MyImpl(0, MyEnum.FOO);
        MyImpl bar = new MyImpl(1, MyEnum.BAR);
        var resolver = new MyResolver(List.of(foo, bar));
        assertThat(resolver.fromSerializedId((byte) 0)).isSameAs(foo);
        assertThat(resolver.fromSerializedId((byte) 1)).isSameAs(bar);

        assertThatThrownBy(() -> resolver.fromSerializedId((byte) 42))
                                                                      .isExactlyInstanceOf(EncryptionException.class)
                                                                      .hasMessage("Unknown MyEnum id: 42");
    }

    @Test
    void toSerializedId() {
        MyImpl foo = new MyImpl(0, MyEnum.FOO);
        MyImpl bar = new MyImpl(1, MyEnum.BAR);
        var resolver = new MyResolver(List.of(foo, bar));

        assertThat(resolver.toSerializedId(foo)).isEqualTo((byte) 0);
        assertThat(resolver.toSerializedId(bar)).isEqualTo((byte) 1);
        MyImpl quux = new MyImpl(2, MyEnum.QUUX);
        assertThatThrownBy(() -> resolver.toSerializedId(quux))
                                                               .isExactlyInstanceOf(EncryptionException.class)
                                                               .hasMessageStartingWith(
                                                                       "Unknown MyEnum impl: io.kroxylicious.filter.encryption.common.AbstractResolverTest$MyImpl@"
                                                               );
    }

    @Test
    void subset() {
        MyImpl foo = new MyImpl(0, MyEnum.FOO);
        MyImpl bar = new MyImpl(1, MyEnum.BAR);
        var resolver = new MyResolver(List.of(foo, bar)).subset(MyEnum.FOO);
        assertThat(resolver.fromName(MyEnum.FOO)).isSameAs(foo);

        // It should no longer know about bar, because we subsetted it
        assertThatThrownBy(() -> resolver.fromName(MyEnum.BAR))
                                                               .isExactlyInstanceOf(EncryptionException.class)
                                                               .hasMessage("Unknown MyEnum name: BAR");

        assertThatThrownBy(() -> resolver.fromSerializedId((byte) 1))
                                                                     .isExactlyInstanceOf(EncryptionException.class)
                                                                     .hasMessage("Unknown MyEnum id: 1");

        assertThatThrownBy(() -> resolver.toSerializedId(bar))
                                                              .isExactlyInstanceOf(EncryptionException.class)
                                                              .hasMessageStartingWith(
                                                                      "Unknown MyEnum impl: io.kroxylicious.filter.encryption.common.AbstractResolverTest$MyImpl@"
                                                              );
    }

    @Test
    void collidingIds() {
        MyImpl foo = new MyImpl(0, MyEnum.FOO);
        MyImpl bar = new MyImpl(0, MyEnum.BAR);
        List<MyInterface> impls = List.of(foo, bar);
        assertThatThrownBy(() -> new MyResolver(impls))
                                                       .isExactlyInstanceOf(IllegalStateException.class)
                                                       .hasMessageContaining("Duplicate key 0");
    }

    @Test
    void collidingNames() {
        MyImpl foo = new MyImpl(0, MyEnum.FOO);
        MyImpl bar = new MyImpl(1, MyEnum.FOO);
        List<MyInterface> impls = List.of(foo, bar);
        assertThatThrownBy(() -> new MyResolver(impls))
                                                       .isExactlyInstanceOf(IllegalStateException.class)
                                                       .hasMessageContaining("Duplicate key FOO");
    }

}
