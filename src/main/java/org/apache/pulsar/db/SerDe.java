package org.apache.pulsar.db;

import java.io.IOException;

public interface SerDe<T> {
    byte[] serialize(T v) throws IOException;

    T deserialize(byte[] payload) throws IOException;
}
