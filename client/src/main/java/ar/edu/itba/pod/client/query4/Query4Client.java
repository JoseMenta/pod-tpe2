package ar.edu.itba.pod.client.query4;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.query2.Query2Client;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query2Result;
import ar.edu.itba.pod.data.results.Query3Result;
import ar.edu.itba.pod.data.results.Query4Result;
import ar.edu.itba.pod.queries.query2.*;
import ar.edu.itba.pod.queries.query3.Query3Collator;
import ar.edu.itba.pod.queries.query3.Query3Combiner;
import ar.edu.itba.pod.queries.query3.Query3Mapper;
import ar.edu.itba.pod.queries.query3.Query3Reducer;
import ar.edu.itba.pod.queries.query4.*;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static ar.edu.itba.pod.Util.QUERY_4_NAMESPACE;

public class Query4Client extends QueryClient {

    private final IMap<String, Infraction> infractionsMap;

    private final MultiMap<LocalDateTime, Ticket> ticketsMap;
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

    private void loadTickets( ){
        loadData(this.ticketPath,
                getMapper(),
                Ticket::getIssueDate,
                i -> i,
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
        final KeyValueSource<LocalDateTime,Ticket> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<LocalDateTime, Ticket> firstJob = tracker.newJob(source);

        Map<Pair<String,String>, Integer> aux = firstJob
                .mapper(new Query4FirstMapper(new Pair<>(from, to)))
                .reducer(new Query4FirstReducer())
                .submit()
                .get();

        auxMap.putAll(aux);
        final KeyValueSource<Pair<String,String>,Integer> secondSource = KeyValueSource.fromMap(auxMap);
        final Job<Pair<String,String>,Integer>  secondJob = tracker.newJob(secondSource);

        return secondJob
                .mapper(new Query4SecondMapper())
                .reducer(new Query4SecondReducer())
                .submit(new Query4Collator())
                .get();
    }


    public static void main(String[] args) {

        try(Query4Client client = new Query4Client("query4")){

            //Load data
            client.loadInfractions();
            client.loadTickets();

            System.out.println("Started");
            //Execute job
            SortedSet<Query4Result> ans = client.executeJob();
            System.out.println("Ended");


            //Print results
            ans.forEach(System.out::println);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }
}
