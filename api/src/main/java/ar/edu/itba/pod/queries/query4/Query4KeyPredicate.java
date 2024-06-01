package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.KeyPredicate;

import java.time.LocalDateTime;

public class Query4KeyPredicate implements KeyPredicate<LocalDateTime> {

//    private final Pair<LocalDateTime, LocalDateTime> dateTimeRange;

    private final LocalDateTime start;

    private final LocalDateTime end;

    public Query4KeyPredicate(Pair<LocalDateTime, LocalDateTime> dateTimeRange) {
        if (dateTimeRange == null) {
            throw new IllegalArgumentException("timeRange cannot be null");
        }
//        this.dateTimeRange = dateTimeRange;
        this.start = dateTimeRange.getFirst();
        this.end = dateTimeRange.getSecond();
    }

    public static boolean isDateTimeInRange(LocalDateTime start, LocalDateTime end, LocalDateTime dateTime) {
        return !dateTime.isBefore(start) && !dateTime.isAfter(end);
    }

    /**
     * Returns true if dateTime is inside the dateTimeRange
     */
    @Override
    public boolean evaluate(LocalDateTime dateTime) {
        return isDateTimeInRange(start,end, dateTime);
    }
}
