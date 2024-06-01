package ar.edu.itba.pod.client.query3;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query1Result;
import ar.edu.itba.pod.data.results.Query3Result;
import ar.edu.itba.pod.queries.query3.Query3Collator;
import ar.edu.itba.pod.queries.query3.Query3Combiner;
import ar.edu.itba.pod.queries.query3.Query3Mapper;
import ar.edu.itba.pod.queries.query3.Query3Reducer;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query3Client extends QueryClient {


    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    private final int cant;

    public Query3Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(Util.QUERY_3_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(Util.QUERY_3_NAMESPACE);
        String cant = System.getProperty("n");
        if (cant == null) {
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
                Ticket::getInfractionCode,
                i -> i,
                ticketsMap::put);
    }

    public SortedSet<Query3Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<String,Ticket> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<String, Ticket> job = tracker.newJob(source);

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

            System.out.println("Started");
            //Execute job
            SortedSet<Query3Result> ans = client.executeJob();
            System.out.println("Ended");

            ans.forEach(System.out::println);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }
}
