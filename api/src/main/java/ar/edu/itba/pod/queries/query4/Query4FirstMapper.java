package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query4FirstMapper implements Mapper<LocalDateTime, Ticket, Pair<String, String>, Integer> {

    // Defines the time range of tickets to consider
    // Should be null if Query4KeyPredicate is used
    private final Pair<LocalDateTime, LocalDateTime> dateTimeRange;

    public Query4FirstMapper(final Pair<LocalDateTime, LocalDateTime> dateTimeRange) {
        final String error = validateRange(dateTimeRange);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        this.dateTimeRange = dateTimeRange;
    }

    private static String validateRange(final Pair<LocalDateTime, LocalDateTime> dateTimeRange) {
        final LocalDateTime start = dateTimeRange.getFirst();
        final LocalDateTime end = dateTimeRange.getSecond();
        if (start == null) {
            return "Start time is null";
        }
        if (end == null) {
            return "End time is null";
        }
        if (!start.isBefore(end)) {
            return "Invalid range";
        }
        return null;
    }

    private Pair<String, String> getKey(final Ticket ticket) {
        return new Pair<>(
                ticket.getNeighbourhood(),
                ticket.getPlate()
        );
    }

    /**
     * Maps the (dateTime, ticket) pair to the (<neighbourhood, plate>, 1) pair
     * if dateTime is included in the timeRange
     */
    @Override
    public void map(LocalDateTime dateTime, Ticket ticket, Context<Pair<String, String>, Integer> context) {
        if (dateTimeRange == null || Query4KeyPredicate.isDateTimeInRange(dateTimeRange, dateTime)) {
            context.emit(getKey(ticket), 1);
        }
    }
}
