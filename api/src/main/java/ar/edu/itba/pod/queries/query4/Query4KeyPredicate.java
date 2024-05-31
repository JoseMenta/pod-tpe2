package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.KeyPredicate;

import java.time.LocalDateTime;

public class Query4KeyPredicate implements KeyPredicate<LocalDateTime> {

    private final Pair<LocalDateTime, LocalDateTime> dateTimeRange;

    public Query4KeyPredicate(Pair<LocalDateTime, LocalDateTime> dateTimeRange) {
        if (dateTimeRange == null) {
            throw new IllegalArgumentException("timeRange cannot be null");
        }
        this.dateTimeRange = dateTimeRange;
    }

    public static boolean isDateTimeInRange(Pair<LocalDateTime, LocalDateTime> dateTimeRange, LocalDateTime dateTime) {
        return !dateTime.isBefore(dateTimeRange.getFirst()) && !dateTime.isAfter(dateTimeRange.getSecond());
    }

    /**
     * Returns true if dateTime is inside the dateTimeRange
     */
    @Override
    public boolean evaluate(LocalDateTime dateTime) {
        return isDateTimeInRange(dateTimeRange, dateTime);
    }
}
