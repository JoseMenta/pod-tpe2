package ar.edu.itba.pod.server;

import ar.edu.itba.pod.Util;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

//Accept repeated key and value multimap
//https://docs.hazelcast.com/hazelcast/5.4/data-structures/multimap#:~:text=Configuring%20MultiMap,-When%20using%20MultiMap&text=Configure%20the%20collection%20type%20with,duplicate%20but%20not%20null%20values.

public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        LOGGER.info("Starting Hazelcast server...");

        //Mask
        String maybeMask = System.getProperty("mask");
        String mask = maybeMask != null ? maybeMask : Util.HAZELCAST_DEFAULT_MASK;
        Collection<String> interfaces = Collections.singletonList(mask);

        //Config
        Config config = new Config();

        //Configure group
        GroupConfig groupConfig = new GroupConfig()
                .setName(Util.HAZELCAST_GROUP_NAME)
                .setPassword(Util.HAZELCAST_GROUP_PASSWORD);
        config.setGroupConfig(groupConfig);

        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName("default");
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        config.addMultiMapConfig(multiMapConfig);

        //Network config
        MulticastConfig multicastConfig = new MulticastConfig();
        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);
        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(interfaces)
                .setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig()
                .setInterfaces(interfacesConfig)
                .setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        // Start cluster
        Hazelcast.newHazelcastInstance(config);
        LOGGER.info("Hazelcast server started");
    }
}
