package ar.edu.itba.pod.client.query3;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;

public class Query3Client extends QueryClient {


    private static final String NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q3";
    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    private final int cant;

    public Query3Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(NAMESPACE);
        String cant = System.getProperty("n");
        if (cant == null) {
            throw new IllegalArgumentException("Missing n parameter");
        }
        this.cant = Integer.parseInt(cant);
    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::code,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets( ){

        switch (this.city){
            case NYC:
                loadData(this.csvPath,
                        this::nyTicketMapper, //TODO: change based on NY or Chicago
                        Ticket::infractionCode,
                        i -> i,
                        ticketsMap::put);
                break;
            case CHI:
                loadData(this.csvPath,
                        this:: chicagoTicketMapper, //TODO: change based on NY or Chicago
                        Ticket::infractionCode,
                        i -> i,
                        ticketsMap::put);
                break;
            default:
                throw new IllegalArgumentException("Invalid city");
        }

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
