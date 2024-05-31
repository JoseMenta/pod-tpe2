package ar.edu.itba.pod.client.query4;

import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.time.LocalDateTime;
import java.util.Map;

import static ar.edu.itba.pod.Util.QUERY_4_NAMESPACE;

public class Query4Client extends QueryClient {

    private final Map<String, Infraction> infractionsMap;
    private final MultiMap<LocalDateTime, Ticket> ticketsMap;
    private final Map<Pair<String, String>, Integer> auxMap;

    public Query4Client() {
        super();
        this.infractionsMap = hazelcast.getMap(QUERY_4_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(QUERY_4_NAMESPACE);
        this.auxMap = hazelcast.getMap(QUERY_4_NAMESPACE + "-aux");
    }

    private void loadInfractions(final String infractionsPath) {
        loadData(
                infractionsPath,
                this::infractionMapper,
                Infraction::code,
                i -> i,
                infractionsMap::put
        );
    }


    private void loadTickets(final String ticketsPath) {
        loadData(
                ticketsPath,
                this::nyTicketMapper,
                Ticket::issueDate,
                t -> t,
                ticketsMap::put
        );
    }

    public static void main(String[] args) {

        try(Query4Client client = new Query4Client()){
        }

    }
}
