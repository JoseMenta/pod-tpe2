package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.data.Infraction;
import ar.edu.itba.pod.client.data.Ticket;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class QueryClient implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    private static final String ADDRESSES = "addresses";
    private static final String ADDRESSES_SEPARATOR = ";";
    private static final String CSV_SEPARATOR = ";";

    protected final HazelcastInstance hazelcast;

    public QueryClient(){
        LOGGER.info("Starting Hazelcast client...");
        this.hazelcast = configureClient();
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
        String[] addresses = System.getProperty(ADDRESSES).split(ADDRESSES_SEPARATOR);
        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);
        return HazelcastClient.newHazelcastClient(clientConfig);

    }

    protected final Ticket nyTicketMapper(String row){
        String[] vals = row.split(CSV_SEPARATOR);
        return new Ticket(
                vals[0],
                LocalDateTime.parse(vals[1], DateTimeFormatter.ISO_DATE),
                vals[2],
                Integer.parseInt(vals[3]),
                vals[4],
                vals[5]
        );
    }

    protected final Ticket chicagoTicketMapper(String row){
        String[] vals = row.split(CSV_SEPARATOR);
        return new Ticket(
                vals[1],
                LocalDateTime.parse(vals[0],DateTimeFormatter.ISO_DATE),
                vals[2],
                Integer.parseInt(vals[4]),
                vals[5],
                vals[3]
        );
    }

    protected final Infraction infractionMapper(String row){
        String[] vals = row.split(CSV_SEPARATOR);
        return new Infraction(
                vals[0],
                vals[1]
        );
    }

    protected final <K,V,D> void loadData(final String csvPath,
                                          Function<String,D> rowMapper,
                                          Function<D,K> keyMapper,
                                          Function<D,V> valueMapper,
                                          BiConsumer<K,V> consumer){
        LOGGER.error("Start loading data from {}",csvPath);
        if(this.hazelcast == null){
            throw new IllegalStateException();
        }
        try (final Stream<String> lines = Files.lines(Path.of(csvPath)).skip(1).parallel()) {
            lines.forEach(l ->{
                D data = rowMapper.apply(l);
                V value = valueMapper.apply(data);
                K key = keyMapper.apply(data); //extract the key from the value, or other data
                consumer.accept(key,value);
            });
            LOGGER.info("Finished loading data for {}",csvPath);
        } catch (IOException e) {
            LOGGER.error("Could not open file {} to load data",csvPath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()  {
       HazelcastClient.shutdownAll();
    }

}
