package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.results.Query5Result;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query5Collator implements Collator<Map.Entry<Integer, List<Pair<String, String>>>, SortedSet<Query5Result>> {

    private static final  Comparator<Pair<String,String>> PAIR_COMPARATOR = Comparator.<Pair<String,String>,String>comparing(Pair::first)
            .thenComparing(Pair::second);

    private static final Comparator<Query5Result> COMPARATOR = Comparator.comparing(Query5Result::group).reversed()
            .thenComparing(Query5Result::tuple,PAIR_COMPARATOR);

    @Override
    public SortedSet<Query5Result> collate(Iterable<Map.Entry<Integer, List<Pair<String, String>>>> values) {
        return StreamSupport.stream(values.spliterator(),false)
                .flatMap(e -> e.getValue().stream().map(p -> new Query5Result(e.getKey(),p)))
                .collect(Collectors.toCollection(()->new TreeSet<>(COMPARATOR)));
//        SortedSet<Query5Result> ans = new TreeSet<>(COMPARATOR);
//        for(Map.Entry<Integer, List<Pair<String, String>>> entry : values){
//            for(Pair<String, String> pair : entry.getValue()){
//                ans.add(new Query5Result(entry.getKey(), pair));
//            }
//        }
//        return ans;
    }
}
