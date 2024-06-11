package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class QueryClient implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryClient.class);

    private static final String ADDRESSES = "addresses";
    private static final String ADDRESSES_SEPARATOR = ";";
    private static final String CSV_SEPARATOR = ";";
    private static final String TICKETS_CSV = "\\tickets";
    private static final String INFRACTIONS_CSV = "\\infractions";
    private static final String TIME_TXT = "time.txt";

    private final FileWriter timeFile;

    protected final String  csvPath;
    protected final String timePath;
    protected final String infractionPath;
    protected final String ticketPath;
    protected final HazelcastInstance hazelcast;
    protected final City city;

    public QueryClient(String query)  {
        LOGGER.info("Starting Hazelcast client...");

        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String cityStr = System.getProperty("city");

        if (inPath == null || outPath == null || cityStr == null) {
            LOGGER.error("Missing parameters");
            throw new IllegalArgumentException("Missing parameters");
        }
        this.city = City.valueOf(cityStr);
        File inFiles = new File(inPath);
        File outFiles = new File(outPath);

        if (!inFiles.exists() || !inFiles.isDirectory() || !outFiles.exists() || !outFiles.isDirectory()) {
            LOGGER.error("Invalid input or output path");
            throw new IllegalArgumentException("Invalid input or output path");
        }

        this.csvPath = outFiles.getAbsolutePath()+"\\"+ query + ".csv";
        this.timePath = outFiles.getAbsolutePath() + File.separator + TIME_TXT;

        this.infractionPath = inFiles.getAbsolutePath() + INFRACTIONS_CSV + this.city.name() + ".csv";
        this.ticketPath = inFiles.getAbsolutePath() + TICKETS_CSV + this.city.name() + ".csv";

        try {
            this.timeFile = new FileWriter(timePath);
        }catch (IOException e){
            LOGGER.error("Could not open file {} to write time",timePath);
            throw new RuntimeException(e);
        }

        LOGGER.info("Paths: csvPath={}, timePath={}, infractionPath={}, ticketPath={}", csvPath, timePath, infractionPath, ticketPath);
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

    protected Function<String, Ticket> getMapper(){
        return switch (this.city){
            case CHI -> this::chicagoTicketMapper;
            case NYC -> this::nyTicketMapper;
        };
    }

    protected final Ticket nyTicketMapper(String row){
        String[] vals = row.split(CSV_SEPARATOR);
        return new Ticket(
                vals[0],
                LocalDateTime.of(LocalDate.parse(vals[1],DateTimeFormatter.ISO_DATE), LocalTime.of(0,0,0)),
                vals[2],
                Double.parseDouble(vals[3]),
                vals[4],
                vals[5]
        );
    }

    protected final Ticket chicagoTicketMapper(String row){
        String[] vals = row.split(CSV_SEPARATOR);
        return new Ticket(
                vals[1],
                LocalDateTime.parse(vals[0],DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                vals[2],
                Double.parseDouble(vals[4]),
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

    protected final <D,K,V> void loadData(final String csvPath,
                                          Function<String,D> rowMapper,
                                          Function<D,K> keyMapper,
                                          Function<D,V> valueMapper,
                                          BiConsumer<K,V> consumer){
        LOGGER.info("Start loading data from {}",csvPath);
        writeTime("Start loading data from " + csvPath);
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
            writeTime("Finished loading data for " + csvPath);
            LOGGER.info("Finished loading data for {}",csvPath);
        } catch (IOException e) {
            writeTime("Finished loading data with error for " + csvPath);
            LOGGER.error("Could not open file {} to load data",csvPath);
            throw new RuntimeException(e);
        }
    }


    protected void writeTime(String message){
        try {
            timeFile.write(LocalDateTime.now() + " " + message + "\n");
        } catch (IOException e) {
            LOGGER.error("Could not write time to file");
            throw new RuntimeException(e);
        }
    }

    protected <T> void writeResults(final List<String> headerTitles,
                                   final Collection<T> rows,
                                   final Function<T,String> rowFormatter){
        try(BufferedWriter bw = Files.newBufferedWriter(
                        Paths.get(this.csvPath),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE
                )){
            bw.write(String.join(";",headerTitles)+"\n");
            for(T e : rows){
                bw.write(rowFormatter.apply(e));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected <T> T execute(Callable<T> supplier) throws Exception{
        this.writeTime("Start MapReduce Job");
        T ans = supplier.call();
        this.writeTime("End MapReduce Job");
        return ans;
    }


    @Override
    public void close()  {
       HazelcastClient.shutdownAll();
        try {
            timeFile.flush();
            timeFile.close();
        } catch (IOException e) {
            LOGGER.error("Could not close time file");
            throw new RuntimeException(e);
        }
    }

}
