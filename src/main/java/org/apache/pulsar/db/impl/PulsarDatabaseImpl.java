package org.apache.pulsar.db.impl;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.db.PulsarDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

@AllArgsConstructor
@Slf4j
public class PulsarDatabaseImpl<V, O> implements PulsarDatabase<V, O> {
    private final PulsarClient pulsarClient;
    private final Function<O, byte[]> opSerializer;
    private final Function<byte[], O> opDeserializer;
    private final BiConsumer<V, O> changeLogApplier;

    private final String topic;
    private final V state;

    private CompletableFuture<Reader<byte[]>> reader;

    private synchronized CompletableFuture<Reader<byte[]>> getReaderHandle() {
        if (reader == null) {
            reader = pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .startMessageIdInclusive()
                    .createAsync();
        }
        return reader;
    }

    private CompletableFuture<?> readNextMessageIfAvailable(Reader<byte[]> reader) {
        return reader
                .hasMessageAvailableAsync()
                .thenCompose(hasMessageAvailable -> {
                    if (hasMessageAvailable == null
                            || !hasMessageAvailable) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        CompletableFuture<Message<byte[]>> opMessage = reader.readNextAsync();
                        // we cannot perform this inside the Netty thread
                        // so we are using here thenComposeAsync
                        return opMessage.thenComposeAsync(msg -> {
                            byte[] value = msg.getValue();
                            applyToLocalStateFromLog(value);
                            return readNextMessageIfAvailable(reader);
                        });
                    }
                });
    }

    private void applyToLocalStateFromLog(byte[] value) {
        O op = opDeserializer.apply(value);
        applyToLocalState(op);
    }

    private void applyToLocalState(O op) {
        changeLogApplier.accept(state, op);
    }

    private CompletableFuture<?> ensureLatestData() {
        CompletableFuture<Reader<byte[]>> readerHandle = getReaderHandle();
        return readerHandle.thenCompose(this::readNextMessageIfAvailable);
    }

    @Override
    public <K> CompletableFuture<K> read(Function<V, K> reader, boolean latest) {
        if (latest) {
            return ensureLatestData().thenApply( a ->  reader.apply(state));
        } else {
            return CompletableFuture.completedFuture(reader.apply(state));
        }
    }

    @Override
    public synchronized <K> CompletableFuture<K> write(Function<V, List<O>> opBuilder, Function<V, K> reader) {
            log.info("opening exclusive producer to {}", topic);
            CompletableFuture<Producer<byte[]>> producerHandle = pulsarClient.newProducer()
                    .enableBatching(false)
                    .topic(topic)
                    .accessMode(ProducerAccessMode.WaitForExclusive)
                    .blockIfQueueFull(true)
                    .createAsync();
            return producerHandle.thenCompose(opProducer -> {
                // nobody can write now to the topic
                // wait for local cache to be up-to-date
                CompletableFuture<K> dummy =  ensureLatestData()
                        .thenCompose((___) -> {
                            // build the Op, this will usually use the contents of the local cache
                            List<O> ops = opBuilder.apply(state);
                            List<CompletableFuture<?>> sendHandles = new ArrayList<>();
                            // write to Pulsar
                            // if the write fails we lost the lock
                            for (O op : ops) {
                                // if "op" is null, then we do not have to write to Pulsar
                                if (op != null) {
                                    log.info("writing {} to Pulsar", op);
                                    byte[] serialized = opSerializer.apply(op);
                                    sendHandles.add(opProducer.sendAsync(serialized)
                                            .thenAccept((msgId) -> {
                                        log.info("written {} as {} to Pulsar", op, msgId);
                                        // write to local memory
                                        applyToLocalState(op);
                                    }));

                                }
                            }

                            return CompletableFuture
                                    .allOf(sendHandles.toArray(new CompletableFuture[0]))
                                    .thenApply(____ -> reader.apply(state));

                        });
                // ensure that we release the exclusive producer in any case
                dummy.whenComplete( (___ , err) -> {
                    opProducer.closeAsync();
                });
                return dummy;
            });

    }

    public static <V, O> PulsarDatabaseBuilder builder(PulsarClient pulsarClient) {
        return new RealPulsarDatabaseBuilder().withPulsarClient(pulsarClient);
    }


    private static class RealPulsarDatabaseBuilder implements PulsarDatabaseBuilder {
        private PulsarClient client;
        private Function<?, byte[]> opSerializer;
        private Function<byte[], ?> opDeserializer;
        private Supplier<?> databaseInitializer;
        private BiConsumer<?, ?> changeLogApplier;
        private String topic;

        public PulsarDatabaseBuilder withPulsarClient(PulsarClient pulsarClient) {
            this.client = pulsarClient;
            return this;
        }

        public PulsarDatabaseBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

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
        public <V, O> PulsarDatabaseBuilder withChangeLogApplier(BiConsumer<V, O> changeLogApplier) {
            this.changeLogApplier = changeLogApplier;
            return this;
        }

        @Override
        public <V,O> PulsarDatabase<V, O> build() {
            Objects.requireNonNull(topic, "Topic is required");
            Objects.requireNonNull(client, "Pulsar client is required");
            return new PulsarDatabaseImpl(client,
                    opSerializer,
                    opDeserializer,
                    changeLogApplier,
                    topic,
                    databaseInitializer.get(),
                    null);
        }
    }


    @Override
    public synchronized void close() {
        if (reader != null) {
            reader.thenAccept(reader -> {
                try {
                    reader.close();
                } catch (Exception err) {
                    // ignore
                }
            });
        }
    }
}
