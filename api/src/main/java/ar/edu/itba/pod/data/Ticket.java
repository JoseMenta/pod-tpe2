package ar.edu.itba.pod.data;

import java.time.LocalDateTime;

//Avoid overriding equals and hashCode
//https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Record.html#equals(java.lang.Object)

public record Ticket(
        String plate,
        LocalDateTime issueDate,
        String infractionCode,
        Integer fineAmount, //Integer to avoid generic problems
        String neighbourhood,
        String agency
) {

}
