package ar.edu.itba.pod.data;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;


@Getter
@NoArgsConstructor
public class Pair<V extends Serializable, U extends Serializable> implements DataSerializable {

    private V first;
    private U second;

    public Pair(V first, U second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(first);
        out.writeObject(second);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        first = (V) in.readObject();
        second = (U) in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(first,second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if(o instanceof Pair<?,?> other){
            return Objects.equals(first, other.first) && Objects.equals(second, other.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
