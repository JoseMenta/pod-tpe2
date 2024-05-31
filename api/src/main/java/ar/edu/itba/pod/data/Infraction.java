package ar.edu.itba.pod.data;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Getter
@NoArgsConstructor
public class Infraction implements DataSerializable {
    private String code;
    private String description;


    public Infraction(String code, String description){
        this.code = code;
        this.description = description;
    }
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(code);
        out.writeUTF(description);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
       this.code = in.readUTF();
       this.description = in.readUTF();
    }

}
