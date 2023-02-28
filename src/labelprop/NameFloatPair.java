package labelprop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NameFloatPair implements WritableComparable<NameFloatPair> {

    private Text name;
    private FloatWritable f;

    public NameFloatPair() {
        name = new Text();
        f = new FloatWritable();
    }

    public NameFloatPair(Text name, FloatWritable f) {
        this.name = name;
        this.f = f;
    }

    public void setWord(String name) {
        this.name.set(name);
    }

    public void setFloat(long freq) {
        this.f.set(freq);
    }

    public Text getName() {
        return name;
    }

    public FloatWritable getFloat() {
        return f;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        f.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        f.write(out);
    }

    @Override
    public int compareTo(NameFloatPair o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return name.toString() + " " + f.toString();
    }
}
