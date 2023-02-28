package phase3;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

public class Phase3Reducer extends Reducer<Text, Text, Text, Text> {
    /**
     * Input
     * key name : Text
     * value <name, freq> : NameFreqPair
     *
     * Output
     * key name : Text
     * value : Text
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float total = 0;
        LinkedList<NameFreqPair> l = new LinkedList<>();
        // sum up the total frequency
        for (Text i : values) {
            StringTokenizer itr = new StringTokenizer(i.toString());
            Text name = new Text(itr.nextToken());
            long freq = Integer.parseInt(itr.nextToken());
            total += freq;
            l.push(new NameFreqPair(name, new LongWritable(freq)));
        }

        StringBuilder res = new StringBuilder("[");
        for (NameFreqPair i : l) {
            res.append(String.format("%s, %f|", i.getName().toString(), i.getFreq().get() / total));
        }
        res.deleteCharAt(res.length() - 1);
        res.append("]");

        context.write(key, new Text(res.toString()));
    }
}