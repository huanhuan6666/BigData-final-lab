package labelprop;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class LabelPropReducer extends Reducer<IntWritable, NameFloatPair, Text, Text> {
    /**
     * Input
     * key label: IntWritable
     * value <name, fij> : NameFloatPair
     * 
     * Output
     * key label : Text
     * value  new line of the label : Text
     */
    public void reduce(IntWritable key, Iterable<NameFloatPair> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder res = new StringBuilder("[");
        for (NameFloatPair p : values) {
            res.append(String.format("%s, %f|", p.getName().toString(), p.getFloat().get()));
        }
        res.deleteCharAt(res.length() - 1);
        res.append("]");

        context.write(new Text(key.toString()), new Text(res.toString()));
    }

}
