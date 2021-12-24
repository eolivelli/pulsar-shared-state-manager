package org.apache.pulsar.db;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface PulsarSharedStateManager<V, O> extends AutoCloseable {

    interface PulsarSharedStateManagerBuilder {

        <O> PulsarSharedStateManagerBuilder withOpSerializer(Function<O, byte[]> opSerializer);
        <O> PulsarSharedStateManagerBuilder withOpDeserializer(Function<byte[], O> opDeserializer);
        <V> PulsarSharedStateManagerBuilder withDatabaseInitializer(Supplier<V> databaseInitializer);
        <V, O> PulsarSharedStateManagerBuilder withChangeLogApplier(BiConsumer<V, O> changeLogApplier);
        <V, O> PulsarSharedStateManager<V, O> build();
    }

    /**
     * Read from the current state.
     * @param reader a function that accesses current state and returns a value
     * @param latest ensure that the value is the latest
     * @return an handle to the result of the operation
     */
    <K> CompletableFuture<K> read(Function<V, K> reader, boolean latest);

    /*
     * Execute a mutation on the state.
     * The operationsGenerator generates a list of mutations to be
     * written to the log, the operationApplier function
     * is executed to mutate the state after each successful write
     * to the log. Finally the reader function can read from
     * the current status before releasing the write lock.
     * @param operationsGenerator generates a list of mutations
     * @param operationApplier apply each mutation to the current state
     * @param reader read from the status while inside the write lock
     * @param <K> the returned datat type
     * @param <O> the operation type
     * @return a handle to the completion of the operation
     */
    <K> CompletableFuture<K> write(Function<V, List<O>> operationsGenerator,
                                     Function<V, K> reader);

    @Override
    void close();
}
