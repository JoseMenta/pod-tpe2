package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Util;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class QueryClient implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    private static final String ADDRESSES = "addresses";
    private  final HazelcastInstance hazelcastInstance;

    public QueryClient(){
        LOGGER.info("Starting Hazelcast client...");
        this.hazelcastInstance  = configureClient();
        LOGGER.info("Hazelcast client started");
    }

    private HazelcastInstance configureClient() {
        // Client Config
        ClientConfig clientConfig = new ClientConfig();
        // Group Config
        GroupConfig groupConfig = new GroupConfig()
                .setName(Util.HAZELCAST_GROUP_NAME)
                .setPassword(Util.HAZELCAST_GROUP_PASSWORD);
        clientConfig.setGroupConfig(groupConfig);
        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        String[] addresses = System.getProperty(ADDRESSES).split(";");
        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Override
    public void close()  {
       HazelcastClient.shutdownAll();
    }

}
