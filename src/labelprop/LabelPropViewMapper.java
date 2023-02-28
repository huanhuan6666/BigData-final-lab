package labelprop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LabelPropViewMapper extends Mapper<LongWritable, Text, Text, NameFloatPair> {
    /**
     * Input
     * key lineno
     * value line: a line of matrix F
     * in form "c1\t[n1: v1|n2: v2|...]"
     * 
     * Output
     * key name: Text
     * value ci,vi : NameFloatPair
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        List<NameFloatPair> res = new LinkedList<>();
        String label = LabelPropMapper.parseLine(value.toString(), res);
        for (NameFloatPair p : res) {
            context.write(p.getName(), new NameFloatPair(new Text(label), p.getFloat()));
        }
    }
}
