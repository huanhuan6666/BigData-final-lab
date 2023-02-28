package labelprop;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class LabelCombineReducer extends Reducer<IntWritable, Text, Text, Text> {
    /**
     * Input
     * key label: IntWritable
     * value name: [Text]
     * 
     * id
     */
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder res = new StringBuilder();
        for(Text name : values) {
            res.append(name.toString() + " ");
        }
        context.write(new Text(key.toString()), new Text(res.toString()));
    }
}
