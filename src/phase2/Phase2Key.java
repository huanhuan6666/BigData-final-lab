package phase2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Phase2Key implements WritableComparable {
    private Text name_1;
    private Text name_2;

    public Phase2Key() {
        this.name_1 = new Text();
        this.name_2 = new Text();
    }

    public Phase2Key(Text name_1, Text name_2) {
        this.name_1 = name_1;
        this.name_2 = name_2;
    }

    @Override
    public int compareTo(Object o) {
        return (name_1.compareTo(((Phase2Key)o).name_1) != 0) ? (name_1.compareTo(((Phase2Key)o).name_1)) : (name_2.compareTo(((Phase2Key)o).name_2));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name_1.write(dataOutput);
        name_2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name_1.readFields(dataInput);
        name_2.readFields(dataInput);
    }

    public String toString() {
        return new String("<" + name_1.toString() + ", " + name_2.toString() + ">");
    }
}