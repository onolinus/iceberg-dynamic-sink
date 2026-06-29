package org.apache.hadoop.util;

import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.util.Arrays;

/**
 * Shim for {@code org.apache.hadoop.util.Sets} which was added in Hadoop 3.4.0.
 * VVR 11.6 ships Hadoop 3.3.x which lacks this class.
 * Provides the factory methods that Iceberg 1.10.0 uses.
 */
public final class Sets {

    private Sets() {}

    public static <E> HashSet<E> newHashSet() {
        return new HashSet<>();
    }

    public static <E> HashSet<E> newHashSetWithExpectedSize(int expectedSize) {
        return new HashSet<>((int) (expectedSize / 0.75f) + 1);
    }

    @SafeVarargs
    public static <E> HashSet<E> newHashSet(E... elements) {
        HashSet<E> set = new HashSet<>(elements.length);
        Collections.addAll(set, elements);
        return set;
    }

    public static <E> HashSet<E> newHashSet(Iterable<? extends E> elements) {
        HashSet<E> set = new HashSet<>();
        for (E e : elements) {
            set.add(e);
        }
        return set;
    }
}
