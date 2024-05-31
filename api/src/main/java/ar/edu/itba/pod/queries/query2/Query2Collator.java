package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.data.results.Query2Result;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2Collator implements Collator<Map.Entry<String, List<String>>, SortedSet<Query2Result>> {

    private static final Comparator<Query2Result> COMPARATOR = Comparator.comparing(Query2Result::neighbourhood);

    @Override
    public SortedSet<Query2Result> collate(Iterable<Map.Entry<String, List<String>>> values) {
        return StreamSupport.stream(values.spliterator(),false)
                .map(e -> new Query2Result(e.getKey(), e.getValue()))
                .collect(Collectors.toCollection(()->new TreeSet<>(COMPARATOR)));
    }
}
