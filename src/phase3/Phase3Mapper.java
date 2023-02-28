package phase3;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import java.util.StringTokenizer;

public class Phase3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * Input
     * key lineno
     * value line
     *
     * Output
     * key name: Text
     * value <name, freq> : NameFreqPair
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "<>, \t");
        Text outKey = new Text(itr.nextToken());
        Text outValueName = new Text(itr.nextToken());
        long outValueFreq = Integer.parseInt(itr.nextToken());
        context.write(outKey, new Text(
                String.format("%s %d", outValueName.toString(), outValueFreq)
        ));
    }
}