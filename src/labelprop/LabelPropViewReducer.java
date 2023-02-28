package labelprop;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class LabelPropViewReducer extends Reducer<Text, NameFloatPair, Text, Text> {
    /**
     * Input
     * key name: Text
     * value <label, fij> : NameFloatPair
     * 
     * Output
     * key label : Text
     * value name : Text
     */
    public void reduce(Text key, Iterable<NameFloatPair> values, Context context)
            throws IOException, InterruptedException {
        float maxSoFar = 0;
        String maxLabel = new String();

        for (NameFloatPair p : values) {
            if (p.getFloat().get() > maxSoFar) {
                maxSoFar = p.getFloat().get();
                maxLabel = p.getName().toString();
            }
        }

        context.write(new Text(maxLabel), key);
    }

}
