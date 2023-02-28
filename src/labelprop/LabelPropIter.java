package labelprop;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.IntWritable;

public class LabelPropIter {
    // parameter:
    // arg[0]: input path
    // arg[1]: output path
    // arg[2]: iteration num
    public static void main(String[] args) throws Exception { // 为任务设定配置文件
        try {
            Configuration conf = new Configuration();
            if (args[2].equals("1")) { // if it's the first iteration
                // then read phase3's output and create the initial F file
                FileSystem fs = FileSystem.get(conf);
                Path outfile = new Path(args[0] + "/f");
                FSDataOutputStream output = fs.create(outfile);

                // for no initial label case, set all label as it itself

                Path inpath = new Path("xiyouji/Phase3Output/");
                // Path inpath = new Path("/labeltest/dummyOutput");
                RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(inpath, false);

                int labelNo = 0;
                Boolean useLimitedLabel = true;
                Random rand = new Random();
                Map<Integer, StringBuilder> outres = new HashMap<>();
                while (listFiles.hasNext()) {
                    LocatedFileStatus file = listFiles.next();
                    if (!file.isFile())
                        continue;
                    LineReader reader = new LineReader(fs.open(file.getPath()), conf);
                    Text line = new Text();
                    while (reader.readLine(line) > 0) {
                        String l = line.toString();
                        List<NameFloatPair> res = new LinkedList<>();
                        String key = LabelPropMapper.parseLine(l, res);
                        if (useLimitedLabel) {
                            labelNo = rand.nextInt(LabelPropMapper.LabelNum) + 1; // set a random initial label
                        } else {
                            labelNo++;
                        }
                        StringBuilder entry = outres.get(labelNo);
                        if (entry == null) {
                            outres.put(labelNo, new StringBuilder(String.format("%s, %f", key, 1.0f)));
                        } else {
                            entry.append(String.format("|%s, %f", key, 1.0f));
                        }
                    }
                }

                for (Integer label : outres.keySet()) {
                    String resStr = String.format("%d\t[%s]\n", label, outres.get(label).toString());
                    output.write(resStr.getBytes());
                }
                output.close();
            }

            Job job = Job.getInstance(conf, "label prop iter" + args[2]);// 新建一个用户定义的Job
            job.setJarByClass(LabelPropIter.class); // 设置执行任务的jar
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(LabelPropMapper.class); // 设置Mapper类
            job.setReducerClass(LabelPropReducer.class); // 设置Reducer类
            job.setOutputKeyClass(Text.class); // 设置job输出的key
            // 设置job输出的value
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(NameFloatPair.class);

            // job.setPartitionerClass(InvertedIndexPartitioner.class);
            // 设置输入文件的路径
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // 设置输出文件的路径
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // 提交任务并等待任务完成
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
