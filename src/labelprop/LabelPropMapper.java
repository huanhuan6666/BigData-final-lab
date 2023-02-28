package labelprop;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;

public class LabelPropMapper extends Mapper<LongWritable, Text, IntWritable, NameFloatPair> {
    private Map<String, List<NameFloatPair>> pMatrix;
    private Map<String, Integer> labeledData; 
    public static int LabelNum = 8;

    public static String parseLine(String line, List<NameFloatPair> result) {
        StringTokenizer itr = new StringTokenizer(line, " []|\t:,");
        String res = itr.nextToken();

        while (itr.hasMoreTokens()) {
            String key = itr.nextToken();
            float val = Float.parseFloat(itr.nextToken());
            result.add(new NameFloatPair(new Text(key), new FloatWritable(val)));
        }
        return res;
    }

    // semi-supervised algorithm, initial labels provided
    private void initLabeledData() {
        labeledData = new HashMap<>();
        labeledData.put("唐僧", 1);
        labeledData.put("孙悟空", 2);
        // labeledData.put("东海龙王", 3);
        
        labeledData.put("猪八戒", 3);
        labeledData.put("沙僧", 4);
        labeledData.put("白龙马", 5);
        labeledData.put("如来佛祖", 6);
        labeledData.put("观音菩萨", 7);
        labeledData.put("玉皇大帝", 8);
        assert labeledData.size() == LabelNum;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pMatrix = new HashMap<>();
        // load the matrix P from the Phase3's output
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("xiyouji/Phase3Output/");
        // Path path = new Path("/labeltest/dummyOutput");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path, false);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            if (!file.isFile())
                continue;
            LineReader reader = new LineReader(fs.open(file.getPath()), conf);
            Text line = new Text();
            while (reader.readLine(line) > 0) {
                String l = line.toString();
                List<NameFloatPair> res = new LinkedList<>();
                String key = parseLine(l, res);
                pMatrix.put(key, res);
            }
        }

        initLabeledData();
    }

    // get the value of the F matrix, according to the labeled data
    private float getFValue(String name, int label, float fval) {
        Integer res = labeledData.get(name);
        if (res == null) {
            // the name is not labeled, just return the fval
            return fval;
        }
        else if (res.intValue() == label) {
            // the label matched
            return 1.0f;
        }
        else {
            return 0.0f;
        }
    }

    /**
     * Input
     * key lineno
     * value line: a line of matrix F
     * in form "c1\t[n1: v1|n2: v2|...]"
     * 
     * Output
     * key label: IntWritable
     * value <name, Fij = dot(Pi,Fj)> : NameFloatPair
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // parse the F line
        Map<String, Float> nameValMap = new HashMap<>();
        List<NameFloatPair> res = new LinkedList<>();
        String label = parseLine(value.toString(), res);
        IntWritable labelRepr = new IntWritable(Integer.parseInt(label));

        // build map
        for (NameFloatPair p : res) {
            nameValMap.put(p.getName().toString(), p.getFloat().get());
        }

        // for i in P, calculate dot(Pi, Fj)
        for (String k : pMatrix.keySet()) {
            List<NameFloatPair> pi = pMatrix.get(k);
            float fij = 0;
            for (NameFloatPair p : pi) {
                Float fval = nameValMap.get(p.getName().toString());
                fval = fval == null ? new Float(0.0) : fval;
                // let the labeled f be original, while others keep
                fval = getFValue(k, labelRepr.get(), fval);
                fij += p.getFloat().get() * fval;
            }
            // let the labeled f be original, while others keep
            fij = getFValue(k, labelRepr.get(), fij);
            context.write(labelRepr, new NameFloatPair(new Text(k), new FloatWritable(fij)));
        }
    }

}
