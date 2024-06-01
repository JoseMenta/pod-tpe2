package ar.edu.itba.pod.queries.query3;

import ar.edu.itba.pod.data.results.Query3Result;
import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query3Collator implements Collator<Map.Entry<String,Integer>, SortedSet<Query3Result>> {

    private final int n;

    public Query3Collator(int n) {
        this.n = n;
    }

    private static final Comparator<Query3Result> COMPARATOR = Comparator
            .comparing(Query3Result::percent)
            .thenComparing(Query3Result::agency);

    @Override
    public SortedSet<Query3Result> collate(Iterable<Map.Entry<String, Integer>> values) {
        SortedSet<Query3Result> results = new TreeSet<>(COMPARATOR);
        final int total = StreamSupport.stream(values.spliterator(), false)
                .map(Map.Entry::getValue)
                .reduce(Integer::sum).orElseThrow(IllegalStateException::new);

        for(Map.Entry<String, Integer> entry : values) {
            results.add(new Query3Result(entry.getKey(), (entry.getValue()/total)*100D));
        }
        return results.stream().limit(n).collect(Collectors.toCollection(()->new TreeSet<>(COMPARATOR)));
    }

}
