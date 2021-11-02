package org.apache.pulsar.db;

import org.apache.pulsar.db.impl.PulsarMapImpl;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface PulsarMap<K,V> extends AutoCloseable {

    static <K,V> PulsarMap<K,V> build(
            PulsarDatabase.PulsarDatabaseBuilder builder,
            SerDe<K> keySerDe,
            SerDe<V> valueSerDe) {
        return new PulsarMapImpl<>(builder, keySerDe, valueSerDe);
    }

    /**
     * Get the value associated to a Key.
     * @param key the key
     * @param latest ensure that the value is the latest
     * @return an handle to the result of the operation
     */
    default CompletableFuture<V> get(K key, boolean latest) {
        return getOrDefault(key, null, latest);
    }

    /**
     * Get the value associated to a Key.
     * @param key the key
     * @param defaultValue a value in case that the key is not bound to any value
     * @param latest ensure that the value is the latest
     * @return an handle to the result of the operation
     */
    CompletableFuture<V> getOrDefault(K key, V defaultValue, boolean latest);

    /**
     * Scan the database
     * @param filter a filter on the key
     * @param processor the function to process the data
     * @param latest ensure that the value observed is the latest
     * @return an handle to the result of the operation
     */
    CompletableFuture<?> scan(Function<K, Boolean> filter, BiConsumer<K, V> processor, boolean latest);

    /**
     * Update a binding, the operation may be executed multiple times, until the operation succeeds.
     * If the operation returns null the  value will be removed
     * @param key the key
     * @param operation a function that modifies the value
     * @return an handle to the completion of the operation
     */
    CompletableFuture<V> update(K key, BiFunction<K, V, V> operation);

    /**
     * Update multiple bindings, the operation may be executed multiple times, until the operation succeeds.
     * For each key the operation returns null the value will be removed
     * @param filter a filter to skip processing some keys and reduce the usage of resources
     * @param operation a function that modifies the value
     * @return a handle to the completion of the operation
     */
    CompletableFuture<?> updateMultiple(Function<K, Boolean> filter, BiFunction<K, V, V> operation);

    /**
     * Delete all bindings.
     * @return a handle to the completion of the operation
     */
    CompletableFuture<?> clear();

    /**
     * List all keys.
     * @param latest ensure that we are up-to-date
     * @return a handle to the completion of the operation
     */
    default CompletableFuture<Collection<K>> listKeys(boolean latest) {
        List<K> result = new CopyOnWriteArrayList<>();
        return scan((k) -> true, (k,v) -> {
            result.add(k);
        }, latest).thenApply(___ -> result);
    }

    /**
     * Delete a binding
     * @param key the key
     * @return a handle to the completion of the operation
     */
    default CompletableFuture<V> delete(K key) {
        return update(key, (k, v) -> null);
    }

    /**
     * Update a binding only if the value matches the expected value
     * @param key the key
     * @param expectedValue the expected value, null means that the binding does not exist
     * @return a handle to the completion of the operation
     */
    default CompletableFuture<V> replace(K key, V expectedValue, V value) {
        return update(key, (k,v ) -> Objects.equals(v, expectedValue) ? value : v);
    }
    /**
     * Update a binding
     * @param key the key
     * @return a handle to the completion of the operation
     */
    default CompletableFuture<V> put(K key, V value) {
        return update(key, (k,v )-> value);
    }

    /**
     * Update a binding only the key is not not bound.
     * @param key the key
     * @return a handle to the completion of the operation
     */
    default CompletableFuture<V> putIfAbsent(K key, V value) {
        return replace(key, null, value);
    }

}
