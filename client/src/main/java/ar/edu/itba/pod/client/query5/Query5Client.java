package ar.edu.itba.pod.client.query5;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query1Result;
import ar.edu.itba.pod.data.results.Query5Result;
import ar.edu.itba.pod.queries.query5.*;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query5Client extends QueryClient {

    private static final List<String> CSV_HEADERS = List.of("Group","Infraction A","Infraction B");

    private final IMap<String, Infraction> infractionsMap;

    private final MultiMap<LocalDateTime, Pair<String,Double>> ticketsMap;

    private final IMap<String, Integer> auxMap;

    public Query5Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(Util.QUERY_5_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(Util.QUERY_5_NAMESPACE);
        this.auxMap = hazelcast.getMap(Util.QUERY_5_NAMESPACE + "-aux");
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
                i -> new Pair<>(i.getInfractionCode(),i.getFineAmount()),
                ticketsMap::put);
    }

    public SortedSet<Query5Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<LocalDateTime,Pair<String,Double>> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<LocalDateTime,Pair<String,Double>> job = tracker.newJob(source);

        Map<String, Integer> aux = job
                .mapper(new Query5FirstMapper())
                .combiner(new Query5Combiner())
                .reducer(new Query5FirstReducer())
                .submit()
                .get();

       auxMap.putAll(aux);
       final KeyValueSource<String,Integer> secondSource = KeyValueSource.fromMap(auxMap);
       final Job<String,Integer> secondJob = tracker.newJob(secondSource);

       return secondJob
               .mapper(new Query5SecondMapper())
               .reducer(new Query5SecondReducer())
               .submit(new Query5Collator())
               .get();
    }

    @Override
    public void close() {
        Optional.ofNullable(this.auxMap).ifPresent(Map::clear);
        Optional.ofNullable(this.infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(this.ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public static void main(String[] args) {

        try(Query5Client client = new Query5Client("query5")){

            //Load data
            client.loadInfractions();
            client.loadTickets();

            //Execute job
            SortedSet<Query5Result> ans = client.execute(client::executeJob);

            //Print results
            client.writeResults(CSV_HEADERS,
                    ans,
                    e -> String.format("%d;%s;%s\n",e.group(),e.tuple().getFirst(),e.tuple().getSecond()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
