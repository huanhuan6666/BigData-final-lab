package phase2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Phase2Reducer extends Reducer<Phase2Key, IntWritable, Text, Text> {
    @Override
    protected void reduce(Phase2Key key, Iterable<IntWritable> values, Reducer<Phase2Key, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        while(values.iterator().hasNext())
            sum += values.iterator().next().get();
        context.write(new Text(key.toString()), new Text(String.valueOf(sum)));
    }
}