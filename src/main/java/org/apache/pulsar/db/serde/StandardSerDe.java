package org.apache.pulsar.db.serde;

import lombok.experimental.UtilityClass;
import org.apache.pulsar.db.SerDe;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

@UtilityClass
public class StandardSerDe {

    public static final SerDe<String> STRING = new StringSerde();
    public static final SerDe<Integer> INTEGER = new IntegerSerde();

    private static class StringSerde implements SerDe<String> {

        private StringSerde() {
        }

        @Override
        public byte[] serialize(String v) throws IOException {
            return v.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(byte[] payload) throws IOException {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    private static class IntegerSerde implements SerDe<Integer> {

        private IntegerSerde() {
        }

        @Override
        public byte[] serialize(Integer v) throws IOException {
            return BigInteger.valueOf(v.longValue()).toByteArray();
        }

        @Override
        public Integer deserialize(byte[] payload) throws IOException {
            return new BigInteger(payload).intValue();
        }
    }

}
