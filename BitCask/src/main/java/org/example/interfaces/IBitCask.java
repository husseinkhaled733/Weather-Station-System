package org.example.interfaces;

import java.util.List;
import java.util.Optional;

public interface IBitCask<K, V> {
    Optional<V> get(K key);

    void put(K key, V value);

    List<K> listKeys();

    <ACC> ACC fold(FoldFunction<K, V, ACC, ACC> F, ACC acc0);

    void merge();

    void sync();

    void close();
}
