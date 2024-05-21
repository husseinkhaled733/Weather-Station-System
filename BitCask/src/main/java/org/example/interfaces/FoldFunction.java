package org.example.interfaces;

@FunctionalInterface
public interface FoldFunction<K, V, ACC0, ACC> {
    ACC apply(K key, V value, ACC0 acc0);
}