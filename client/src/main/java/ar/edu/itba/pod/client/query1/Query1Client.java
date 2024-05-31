package ar.edu.itba.pod.client.query1;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.data.Infraction;
import ar.edu.itba.pod.client.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;
import java.util.Optional;

public class Query1Client extends QueryClient {

    private static final String NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q1";

    //TODO: ver si no es mejor guardar directo <String, String>, lo hago asi por ahora (debería cambiarse el keyMapper)
    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    public Query1Client(){
        super();
        this.infractionsMap = hazelcast.getMap(NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(NAMESPACE);
    }

    private void loadInfractions(final String infractionsPath){
        loadData(infractionsPath,
                this::infractionMapper,
                Infraction::code,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets(final String ticketsPath){
        loadData(ticketsPath,
                this::nyTicketMapper, //TODO: change based on NY or Chicago
                Ticket::infractionCode,
                i -> i,
                ticketsMap::put);
    }

    @Override
    public void close() {
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public static void main(String[] args) {
        try(Query1Client client = new Query1Client()){
            //Load data
            client.loadInfractions("hola");
            client.loadTickets("chau");
        }



    }



}
