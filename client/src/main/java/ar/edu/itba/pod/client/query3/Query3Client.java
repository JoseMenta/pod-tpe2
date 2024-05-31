package ar.edu.itba.pod.client.query3;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;

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

    @Override
    public void close() {
        super.close();
    }

    public static void main(String[] args) {

        try(Query3Client client = new Query3Client("query3")){

            //Load data
            client.loadInfractions();
            client.loadTickets();
        }


    }
}
