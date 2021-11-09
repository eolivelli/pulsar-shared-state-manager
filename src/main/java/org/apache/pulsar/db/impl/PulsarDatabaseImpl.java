package org.apache.pulsar.db.impl;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.db.PulsarDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    private CompletableFuture<?> currentReadHandle;

    private synchronized CompletableFuture<Reader<byte[]>> getReaderHandle() {
        return reader;
    }

    private synchronized CompletableFuture<Reader<byte[]>> ensureReaderHandle() {
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

    // visible for testing
    synchronized CompletableFuture<?> ensureLatestData() {
        return ensureLatestData(false);
    }

    synchronized CompletableFuture<?> ensureLatestData(boolean beforeWrite) {
        if (currentReadHandle != null) {
            if (beforeWrite) {
                // we are inside a write loop, so
                // we must ensure that we start to read now
                // otherwise the write would use non up-to-date data
                // so let's finish the current loop
                log.info("A read was already pending, starting a new one in order to ensure consistency");
                return currentReadHandle
                        .thenCompose(___ -> ensureLatestData(false));
            }
            // if there is an ongoing read operation then complete it
            return currentReadHandle;
        }
        // please note that the read operation is async,
        // and it is not execute inside this synchronized block
        CompletableFuture<Reader<byte[]>> readerHandle = ensureReaderHandle();
        final CompletableFuture<?> newReadHandle
                = readerHandle.thenCompose(this::readNextMessageIfAvailable);
        currentReadHandle = newReadHandle;
        return newReadHandle.whenComplete((a, b) -> {
            endReadLoop(newReadHandle);
            if (b != null) {
                throw new CompletionException(b);
            }
        });
    }

    private synchronized void endReadLoop(CompletableFuture<?> handle) {
        if (handle == currentReadHandle) {
            currentReadHandle = null;
        }
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
                CompletableFuture<K> dummy =  ensureLatestData(true)
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

    public static RealPulsarDatabaseBuilder builder() {
        return new RealPulsarDatabaseBuilder();
    }


    public static class RealPulsarDatabaseBuilder implements PulsarDatabaseBuilder {
        private PulsarClient client;
        private Function<?, byte[]> opSerializer;
        private Function<byte[], ?> opDeserializer;
        private Supplier<?> databaseInitializer;
        private BiConsumer<?, ?> changeLogApplier;
        private String topic;

        public RealPulsarDatabaseBuilder withPulsarClient(PulsarClient pulsarClient) {
            this.client = pulsarClient;
            return this;
        }

        public RealPulsarDatabaseBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        @Override
        public <O> RealPulsarDatabaseBuilder withOpSerializer(Function<O, byte[]> opSerializer) {
            this.opSerializer = opSerializer;
            return this;
        }

        @Override
        public <O> RealPulsarDatabaseBuilder withOpDeserializer(Function<byte[], O> opDeserializer) {
            this.opDeserializer = opDeserializer;
            return this;
        }

        @Override
        public <V> RealPulsarDatabaseBuilder withDatabaseInitializer(Supplier<V> databaseInitializer) {
            this.databaseInitializer = databaseInitializer;
            return this;
        }

        @Override
        public <V, O> RealPulsarDatabaseBuilder withChangeLogApplier(BiConsumer<V, O> changeLogApplier) {
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
                    null,
                    null);
        }
    }


    @Override
    public void close() {
        CompletableFuture<Reader<byte[]>> readerHandle = getReaderHandle();
        if (readerHandle != null) {
            readerHandle.thenAccept(reader -> {
                try {
                    reader.close();
                } catch (Exception err) {
                    // ignore
                }
            });
        }
    }
}
