package org.apache.pulsar.db;

import org.apache.pulsar.db.impl.MemoryDatabaseImpl;
import org.apache.pulsar.db.serde.StandardSerDes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PulsarMapImplTest {

    @Test
    public void basicTest() throws Exception {
        PulsarMap<String, Integer> map = PulsarMap.build(
                MemoryDatabaseImpl.builder(),
                StandardSerDes.STRING,
                StandardSerDes.INTEGER);

        map.put("a", 1).get();
        assertEquals(1, map.get("a", false).get());
        assertEquals(1, map.get("a", true).get());

        assertEquals(2, map.getOrDefault("b", 2, true).get());
        assertEquals(2, map.getOrDefault("b", 2, false).get());

        assertEquals(3, map.putIfAbsent("c", 3).get());
        assertEquals(3, map.putIfAbsent("c", 4).get());
        assertEquals(5, map.replace("c", 3, 5).get());
        assertEquals(5, map.replace("c", 6, 7).get());
        map.clear().get();

        assertNull(map.get("a", true).get());
        assertNull(map.get("a", false).get());

        map.put("f", 9).get();
        map.delete("f").get();
        assertNull(map.get("f", true).get());

    }
}
