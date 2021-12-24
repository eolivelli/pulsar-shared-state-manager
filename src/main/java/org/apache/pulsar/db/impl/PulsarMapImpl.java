package org.apache.pulsar.db.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.db.PulsarSharedStateManager;
import org.apache.pulsar.db.PulsarMap;
import org.apache.pulsar.db.SerDe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class PulsarMapImpl <K,V> implements PulsarMap<K,V> {
    private static ObjectMapper mapper = new ObjectMapper();
    private final PulsarSharedStateManager<Map<K,V>, MapOp> stateManager;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;

    public PulsarMapImpl(PulsarSharedStateManager.PulsarSharedStateManagerBuilder builder,
                         SerDe<K> keySerDe,
                         SerDe<V> valueSerDe
    ) {
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.stateManager = builder
                .withOpSerializer(this::serializeOp)
                .withOpDeserializer(this::deserializeOp)
                .withDatabaseInitializer(() -> new ConcurrentHashMap<K,V>())
                .<Map<K,V>, MapOp>withChangeLogApplier(this::applyOp)
                .build();
    }

    @Override
    public CompletableFuture<V> getOrDefault(K key, V defaultValue, boolean latest) {
        return stateManager.read(map -> map.getOrDefault(key, defaultValue), latest);
    }

    @Override
    public CompletableFuture<?> scan(Function<K, Boolean> filter, BiConsumer<K, V> processor, boolean latest) {
        return stateManager.read(map -> {
            map.forEach((k,v) -> {
                if (filter.apply(k)) {
                    processor.accept(k, v);
                }
            });
            return null;
        }, latest);
    }

    @Override
    public CompletableFuture<V> update(K key, BiFunction<K, V, V> operation) {
        return stateManager.write(map -> {
            V currentValue = map.get(key);
            V finalValue = operation.apply(key, currentValue);
            if (finalValue == null) {
                return Collections.singletonList(MapOp.DELETE(key));
            } else {
                return Collections.singletonList(MapOp.PUT(key, finalValue));
            }
        }, map -> map.get(key));
    }

    @Override
    public CompletableFuture<?> updateMultiple(Function<K, Boolean> filter, BiFunction<K, V, V> operation) {
        return stateManager.write(map -> {
            List<MapOp> updates = new ArrayList<>();
            map.forEach((key, currentValue) -> {
                V finalValue = operation.apply(key, currentValue);
                if (finalValue == null) {
                    updates.add(MapOp.DELETE(key));
                } else {
                    updates.add(MapOp.PUT(key, finalValue));
                }
            });
            return updates;
        }, null);
    }

    @Override
    public CompletableFuture<?> clear() {
        return stateManager.write(map -> {
            return Collections.singletonList(MapOp.CLEAR());
        }, Function.identity());
    }

    @AllArgsConstructor
    @Data
    private static class MapOp<K,V> {
        private final static int TYPE_CLEAR = 0;
        private final static int TYPE_PUT =  1;
        private final static int TYPE_DELETE =  2;
        private final int type;
        private final K key;
        private final V value;

        static <K,V> MapOp<K,V> CLEAR() {
            return new MapOp(TYPE_CLEAR, null, null);
        }
        static <K,V> MapOp<K,V> PUT(K key, V value) {
            return new MapOp(TYPE_PUT, key, value);
        }
        static <K,V> MapOp<K,V> DELETE(K key) {
            return new MapOp(TYPE_DELETE, key, null);
        }
    }

    public <K,V> void applyOp(Map<K, V> map, MapOp<K,V> op) {
        switch (op.getType()) {
            case MapOp.TYPE_CLEAR:
                map.clear();
                break;
            case MapOp.TYPE_PUT:
                map.put(op.getKey(), op.getValue());
                break;
            case MapOp.TYPE_DELETE:
                map.remove(op.getKey());
                break;
            default:
                log.warn("Ignore MapOp {} on {}", op, this);
                break;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static final class SerializedMapOp {
        private int type;
        private byte[] key;
        private byte[] value;
    }

    private byte[] serializeOp(MapOp<K,V> op) {
        try {
            SerializedMapOp ser = new SerializedMapOp(op.getType(),
                    op.getKey() != null ? keySerDe.serialize(op.getKey()) : null,
                    op.getValue() != null ? valueSerDe.serialize(op.getValue()) : null);
            return mapper.writeValueAsBytes(ser);
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    private MapOp<K,V> deserializeOp(byte[] value) {
        try {
            SerializedMapOp ser =  mapper.readValue(value, SerializedMapOp.class);
            return new MapOp<K, V>(ser.getType(),
                    ser.getKey() != null ? keySerDe.deserialize(ser.getKey()) : null,
                    ser.getValue() != null ? valueSerDe.deserialize(ser.getValue()) : null);
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    @Override
    public void close() {
        stateManager.close();
    }
}
