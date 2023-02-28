package labelprop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

public class LabelCombineMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    /**
     * Input
     * key lineno
     * value label, name pair
     * in form "c1\t[n1: v1|n2: v2|...]"
     * 
     * Output
     * key label: IntWritable
     * value name: Text
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        IntWritable k = new IntWritable(Integer.parseInt(itr.nextToken()));
        context.write(k, new Text(itr.nextToken()));
    }
}
