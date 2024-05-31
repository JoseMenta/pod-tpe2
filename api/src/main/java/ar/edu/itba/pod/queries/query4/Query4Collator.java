package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.results.Query4Result;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query4Collator implements Collator<Map.Entry<String, Pair<String, Integer>>, SortedSet<Query4Result>> {

    private static final Comparator<Query4Result> COMPARATOR = Comparator.comparing(Query4Result::neighbourhood);

    /**
     * Given the (neighbourhood, <plate, total>) pair for each neighbourhood, it orders them by the neighbourhood in ascending order
     */
    @Override
    public SortedSet<Query4Result> collate(Iterable<Map.Entry<String, Pair<String, Integer>>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .map(e -> new Query4Result(e.getKey(), e.getValue().first(), e.getValue().second()))
                .collect(Collectors.toCollection(() -> new TreeSet<>(COMPARATOR)));
    }
}
