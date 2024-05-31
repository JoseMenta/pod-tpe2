package ar.edu.itba.pod.client.query4;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class Query4Client extends QueryClient {

    private static final String NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q4";
    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    private final LocalDate from;
    private final LocalDate to;

    public Query4Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(NAMESPACE);
        String fromString = System.getProperty("from");
        String toString = System.getProperty("to");
        if (fromString == null || toString == null) {
            throw new IllegalArgumentException("Missing from or to parameter");
        }

        this.from = LocalDate.parse(fromString, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        this.to = LocalDate.parse(toString, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::code,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets( ){

        switch (city){
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

        try(Query4Client client = new Query4Client("query4")){

            //Load data
            client.loadInfractions();
            client.loadTickets();
        }


    }
}
