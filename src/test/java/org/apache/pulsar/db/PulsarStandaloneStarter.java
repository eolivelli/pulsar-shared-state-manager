package org.apache.pulsar.db;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.io.File;
import java.util.Collections;

public class PulsarStandaloneStarter implements AutoCloseable {

    private final File tempDir;

    private PulsarStandalone pulsarBroker;
    @Getter
    private PulsarClient pulsarClient;
    @Getter
    private PulsarAdmin admin;

    public PulsarStandaloneStarter(File tempDir) {
        this.tempDir = tempDir;
    }

    public void start() throws Exception {
        pulsarBroker = new PulsarStandalone();
        pulsarBroker.setNoFunctionsWorker(true);
        pulsarBroker.setNoStreamStorage(true);
        pulsarBroker.setNumOfBk(1);
        pulsarBroker.setBkDir(tempDir.getAbsolutePath());
        pulsarBroker.setZkDir(tempDir.getAbsolutePath());
        pulsarBroker.setConfigFile("src/test/resources/standalone.conf");
        ServiceConfiguration config = new ServiceConfiguration();
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setZookeeperServers("localhost:2181");
        config.setClusterName("standalone");
        config.setRunningStandalone(true);
        pulsarBroker.setConfig(config);
        pulsarBroker.start();

        pulsarClient = PulsarClient
            .builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();
        admin = PulsarAdmin
                 .builder()
                 .serviceHttpUrl("http://localhost:8080")
                 .build();
//        admin.clusters().createCluster("standalone",
//                ClusterData.builder()
//                        .serviceUrl("http://localhost:8080")
//                        .build());
//        admin.tenants().createTenant("public", TenantInfo
//                .builder()
//                .allowedClusters(Collections.singleton("standalone"))
//                .build());
//        admin.namespaces().createNamespace("public/default");
    }

    public void close() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        if (pulsarBroker != null) {
            pulsarBroker.close();
        }
    }
}
