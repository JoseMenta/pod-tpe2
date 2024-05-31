package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.List;

public class Query5SecondReducer implements ReducerFactory<Integer, String, List<Pair<String, String>>> {
    @Override
    public Reducer<String, List<Pair<String, String>>> newReducer(Integer i) {
        return new Reducer<String, List<Pair<String, String>>>() {

            List<String> infractionCodes;

            @Override
            public void beginReduce() {
                this.infractionCodes = new ArrayList<>();
            }

            @Override
            public void reduce(String s) {
                infractionCodes.add(s);
            }

            @Override
            public List<Pair<String, String>> finalizeReduce() {
                List<Pair<String, String>> infractionCodesPair = new ArrayList<>();
                for (int i = 0; i < infractionCodes.size(); i++) {
                    for (int j = i+1; j < infractionCodes.size(); j++) {
                        infractionCodesPair.add(new Pair<>(infractionCodes.get(i), infractionCodes.get(j)));
                    }
                }
                return infractionCodesPair;
            }
        };
    }

}
