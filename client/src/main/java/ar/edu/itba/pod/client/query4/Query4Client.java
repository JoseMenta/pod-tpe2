package ar.edu.itba.pod.client.query4;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query1Result;
import ar.edu.itba.pod.data.results.Query4Result;
import ar.edu.itba.pod.queries.query4.*;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static ar.edu.itba.pod.Util.QUERY_4_NAMESPACE;

public class Query4Client extends QueryClient {

    private static final List<String> CSV_HEADERS = List.of("County","Plate","Tickets");

    private final IMap<String, Infraction> infractionsMap;

    private final MultiMap<LocalDateTime, Pair<String,String>> ticketsMap;
    private final IMap<Pair<String, String>, Integer> auxMap;

    private final LocalDateTime from;
    private final LocalDateTime to;

    public Query4Client(String query) {
        super(query);
        this.auxMap = hazelcast.getMap(QUERY_4_NAMESPACE + "-aux");
        this.infractionsMap = hazelcast.getMap(QUERY_4_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(QUERY_4_NAMESPACE);
        String fromString = System.getProperty("from");
        String toString = System.getProperty("to");
        if (fromString == null || toString == null) {
            throw new IllegalArgumentException("Missing from or to parameter");
        }

        this.from = LocalDate.parse(fromString, DateTimeFormatter.ofPattern("dd/MM/yyyy")).atStartOfDay();
        this.to = LocalDate.parse(toString, DateTimeFormatter.ofPattern("dd/MM/yyyy")).atStartOfDay();
    }

    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::getCode,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets(){
//        loadData(this.ticketPath,
//                getMapper(),
//                Ticket::getIssueDate,
//                i -> new Pair<>(i.getNeighbourhood(),i.getPlate()),
//                ticketsMap::put);
        final KeyPredicate<LocalDateTime> predicate = new Query4KeyPredicate(new Pair<>(from, to));
        loadDataWithPredicate(this.ticketPath,
                getMapper(),
                Ticket::getIssueDate,
                predicate::evaluate,
                i -> new Pair<>(i.getNeighbourhood(),i.getPlate()),
                ticketsMap::put);

    }

    @Override
    public void close() {
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        Optional.ofNullable(auxMap).ifPresent(Map::clear);
        super.close();
    }


    public SortedSet<Query4Result> executeJob() throws ExecutionException, InterruptedException {

        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<LocalDateTime,Pair<String,String>> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<LocalDateTime, Pair<String,String>> firstJob = tracker.newJob(source);

        SortedSet<Query4Result> aux = firstJob
//                .keyPredicate(new Query4KeyPredicate(new Pair<>(from, to)))
                .mapper(new Query4FirstMapper(new Pair<>(from, to)))
                .reducer(new Query4FirstReducer())
                .submit(new Query4Collator())
                .get();

        return aux;
    }


    public static void main(String[] args) {

        try(Query4Client client = new Query4Client("query4")){

            //Load data
            client.loadInfractions();
            client.loadTickets();

            //Execute job
            SortedSet<Query4Result> ans = client.execute(client::executeJob);


            //Print results
            client.writeResults(CSV_HEADERS,
                    ans,
                    e -> String.format("%s;%s;%d\n",e.neighbourhood(),e.plate(),e.total()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
