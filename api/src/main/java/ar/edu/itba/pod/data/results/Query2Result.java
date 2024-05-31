package ar.edu.itba.pod.data.results;

import java.util.List;

public record Query2Result (
        String neighbourhood,
        List<String> infractions
){

}
