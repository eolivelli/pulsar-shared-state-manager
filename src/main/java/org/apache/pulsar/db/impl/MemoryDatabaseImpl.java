package org.apache.pulsar.db.impl;

import lombok.AllArgsConstructor;
import org.apache.pulsar.db.PulsarDatabase;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

@AllArgsConstructor
public class MemoryDatabaseImpl<V, O> implements PulsarDatabase<V, O> {

    private final Function<O, byte[]> opSerializer;
    private final Function<byte[], O> opDeserializer;
    private final BiConsumer<V, O> changeLogApplier;
    private final V state;


    @Override
    public <K> CompletableFuture<K> read(Function<V, K> reader, boolean latest) {
        return CompletableFuture.supplyAsync(() -> reader.apply(state));
    }

    @Override
    public <K> CompletableFuture<K> write(Function<V, List<O>> operationsGenerator, Function<V, K> reader) {
        return CompletableFuture.supplyAsync(() -> {
            List<O> ops = operationsGenerator.apply(state);
            for (O op : ops) {
                O rewritten = opDeserializer.apply(opSerializer.apply(op));
                changeLogApplier.accept(state, rewritten);
            }
            if (reader == null) {
                return null;
            }
            return reader.apply(state);
        });
    }

    public static PulsarDatabaseBuilder builder() {
        return new MockPulsarDatabaseBuilder();
    }


    private static class MockPulsarDatabaseBuilder implements PulsarDatabaseBuilder {
        private Function<?, byte[]> opSerializer;
        private Function<byte[], ?> opDeserializer;
        private Supplier<?> databaseInitializer;
        private BiConsumer<?, ?> changeLogApplier;

        @Override
        public <O> PulsarDatabaseBuilder withOpSerializer(Function<O, byte[]> opSerializer) {
            this.opSerializer = opSerializer;
            return this;
        }

        @Override
        public <O> PulsarDatabaseBuilder withOpDeserializer(Function<byte[], O> opDeserializer) {
            this.opDeserializer = opDeserializer;
            return this;
        }

        @Override
        public <V> PulsarDatabaseBuilder withDatabaseInitializer(Supplier<V> databaseInitializer) {
            this.databaseInitializer = databaseInitializer;
            return this;
        }

        @Override
        public <V,O> PulsarDatabaseBuilder withChangeLogApplier(BiConsumer<V, O> changeLogApplier) {
            this.changeLogApplier = changeLogApplier;
            return this;
        }

        @Override
        public <V,O> PulsarDatabase<V, O> build() {
            return new MemoryDatabaseImpl(opSerializer, opDeserializer, changeLogApplier, databaseInitializer.get());
        }
    }

    @Override
    public void close() {
    }
}
