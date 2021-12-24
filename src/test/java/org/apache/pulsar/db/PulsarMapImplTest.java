package org.apache.pulsar.db;

import org.apache.pulsar.db.impl.MemorySharedStateManagerImpl;
import org.apache.pulsar.db.impl.PulsarSharedStateManagerImpl;
import org.apache.pulsar.db.serde.StandardSerDe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PulsarMapImplTest {

    @TempDir
    private File tempDir;

    @Test
    public void basicTest() throws Exception {
        try (PulsarMap<String, Integer> map = PulsarMap.build(
                MemorySharedStateManagerImpl.builder(),
                StandardSerDe.STRING,
                StandardSerDe.INTEGER);) {

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

    @Test
    public void basicTestWithPulsar() throws Exception {
        try (PulsarStandaloneStarter pulsarBroker = new PulsarStandaloneStarter(tempDir)) {
            pulsarBroker.start();

            try (PulsarMap<String, Integer> map = PulsarMap.build(
                    PulsarSharedStateManagerImpl
                            .builder()
                            .withPulsarClient(pulsarBroker.getPulsarClient())
                            .withTopic("persistent://public/default/mymap"),
                    StandardSerDe.STRING,
                    StandardSerDe.INTEGER);) {

                map.put("a", 1).get();
                assertEquals(1, map.get("a", false).get());

                // open another instance, ensure that we can read the data
                try (PulsarMap<String, Integer> map2 = PulsarMap.build(
                        PulsarSharedStateManagerImpl
                                .builder()
                                .withPulsarClient(pulsarBroker.getPulsarClient())
                                .withTopic("persistent://public/default/mymap"),
                        StandardSerDe.STRING,
                        StandardSerDe.INTEGER);) {

                    assertEquals(1, map2.get("a", true).get());


                    // write from the second instance
                    map2.put("a", 2).get();
                    assertEquals(2, map2.get("a", false).get());

                    // read from the first instance
                    assertEquals(2, map.get("a", true).get());
                }

            }


        }

    }
}
