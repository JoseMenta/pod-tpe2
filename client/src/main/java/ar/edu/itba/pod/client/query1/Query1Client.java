package ar.edu.itba.pod.client.query1;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query1Result;
import ar.edu.itba.pod.queries.query1.Query1Collator;
import ar.edu.itba.pod.queries.query1.Query1Combiner;
import ar.edu.itba.pod.queries.query1.Query1Mapper;
import ar.edu.itba.pod.queries.query1.Query1Reducer;
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

import static ar.edu.itba.pod.Util.QUERY_1_NAMESPACE;

public class Query1Client extends QueryClient {

    private static final List<String> CSV_HEADERS = List.of("Infraction","Tickets");

    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, String> ticketsMap;

    public Query1Client(String query){
        super(query);
        this.infractionsMap = hazelcast.getMap(QUERY_1_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(QUERY_1_NAMESPACE);
    }

    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::getCode,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets(){
        loadData(this.ticketPath,
                getMapper(),
                Ticket::getInfractionCode,
                Ticket::getInfractionCode, //TODO: check if loading a 1 is valid
                ticketsMap::put);
    }

    @Override
    public void close() {
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public SortedSet<Query1Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<String,String> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<String, String> job = tracker.newJob(source);

        return job
                .mapper(new Query1Mapper())
                .combiner(new Query1Combiner())
                .reducer(new Query1Reducer())
                .submit(new Query1Collator(infractionsMap))
                .get();
    }

    public static void main(String[] args) {

        try(
                Query1Client client = new Query1Client("query1");
        ){
            //Load data
            client.loadInfractions();
            client.loadTickets();

            //Execute job
            SortedSet<Query1Result> ans = client.execute(client::executeJob);

            // Write to CSV
            client.writeResults(CSV_HEADERS,
                    ans,
                    v -> String.format("%s;%d\n",v.infraction(),v.tickets()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }




}
