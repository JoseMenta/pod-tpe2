package ar.edu.itba.pod.client.query3;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query3Result;
import ar.edu.itba.pod.queries.query3.Query3Collator;
import ar.edu.itba.pod.queries.query3.Query3Combiner;
import ar.edu.itba.pod.queries.query3.Query3Mapper;
import ar.edu.itba.pod.queries.query3.Query3Reducer;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query3Client extends QueryClient {

    private static final List<String> CSV_HEADERS = List.of("Issuing Agency","Percentage");

    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<LocalDateTime, Pair<String, Double>> ticketsMap;

    private final int cant;

    public Query3Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(Util.QUERY_3_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(Util.QUERY_3_NAMESPACE);
        String cant = System.getProperty("n");
        if (cant == null) {
            this.close();
            throw new IllegalArgumentException("Missing n parameter");
        }
        this.cant = Integer.parseInt(cant);
    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::getCode,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets( ){
        loadData(this.ticketPath,
                getMapper(),
                Ticket::getIssueDate,
                i -> new Pair<>(i.getAgency(), i.getFineAmount()),
                ticketsMap::put);
    }

    public SortedSet<Query3Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<LocalDateTime,Pair<String, Double>> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<LocalDateTime, Pair<String, Double>> job = tracker.newJob(source);

        return job
                .mapper(new Query3Mapper())
                .combiner(new Query3Combiner())
                .reducer(new Query3Reducer())
                .submit(new Query3Collator(cant))
                .get();
    }

    @Override
    public void close() {
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public static void main(String[] args) {

        try(Query3Client client = new Query3Client("query3")){

            //Load data
            client.loadInfractions();
            client.loadTickets();

            //Execute job
            //SortedSet<Query3Result> ans = client.executeJob();
            SortedSet<Query3Result> ans = client.execute(client::executeJob);

            client.writeResults(CSV_HEADERS,
                    ans,
                    e->String.format("%s;%.2f%%\n",e.agency(),e.percent()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
