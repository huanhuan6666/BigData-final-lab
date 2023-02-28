package phase3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NameFreqPair implements WritableComparable<NameFreqPair> {

    private Text name;
    private LongWritable freq;

    public NameFreqPair() {
        name = new Text();
        freq = new LongWritable();
    }

    public NameFreqPair(Text name, LongWritable freq) {
        this.name = name;
        this.freq = freq;
    }

    public void setWord(String name) {
        this.name.set(name);
    }

    public void setFreq(long freq) {
        this.freq.set(freq);
    }

    public Text getName() {
        return name;
    }

    public LongWritable getFreq() {
        return freq;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        freq.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        freq.write(out);
    }

    @Override
    public int compareTo(NameFreqPair o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return name.toString() + " " + freq.toString();
    }
}