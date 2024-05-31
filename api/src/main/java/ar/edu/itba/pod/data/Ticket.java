package ar.edu.itba.pod.data;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

//Avoid overriding equals and hashCode
//https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Record.html#equals(java.lang.Object)

@Getter
public class Ticket implements DataSerializable {

    private String plate;
    private LocalDateTime issueDate;
    private String infractionCode;
    private Integer fineAmount; //Integer to avoid generic problems
    private String neighbourhood;
    private String agency;

    public Ticket(String plate, LocalDateTime issueDate, String infractionCode, Integer fineAmount, String neighbourhood, String agency) {
        this.plate = plate;
        this.issueDate = issueDate;
        this.infractionCode = infractionCode;
        this.fineAmount = fineAmount;
        this.neighbourhood = neighbourhood;
        this.agency = agency;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(plate);
        out.writeUTF(issueDate.format(DateTimeFormatter.ISO_DATE));
        out.writeUTF(infractionCode);
        out.writeInt(fineAmount);
        out.writeUTF(neighbourhood);
        out.writeUTF(agency);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        plate = in.readUTF();
        issueDate = LocalDateTime.parse(in.readUTF(), DateTimeFormatter.ISO_DATE);
        infractionCode = in.readUTF();
        fineAmount = in.readInt();
        neighbourhood = in.readUTF();
        agency = in.readUTF();
    }
}
