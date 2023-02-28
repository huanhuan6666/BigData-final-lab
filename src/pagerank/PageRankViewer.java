package pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class PageRankViewer {
    public static class DecDoubleWritable extends DoubleWritable {
        public DecDoubleWritable() { }
        public DecDoubleWritable(double value) {
            this.set(value);
        }

        @Override
        public int compareTo(DoubleWritable o) {
            return -super.compareTo(o);
        }
    }

    public static class RankViewerMapper extends Mapper<LongWritable, Text, DecDoubleWritable, Text> {
        /*
         * 输入PageRankIter的迭代结果 输出PR从大到小排序结果
         * Input:
         *  key:lineoff value:唐僧\tcur_PR$[悟空, 0.5|八戒, 0.5]
         * Output:
         *  key:cur_PR value:唐僧
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] temp1 = line.split("\t");
            String[] temp2 = temp1[1].split("\\$");
            String name = temp1[0];
            double curPR = Double.parseDouble(temp2[0]);

            // 想要利用shuffle将PR值排序 则必须将PR放到key中
            context.write(new DecDoubleWritable(curPR), new Text(name));
        }
    }

    public static class RankViewerReducer extends Reducer<DecDoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DecDoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 可能同一个PR值有好几个人名
            for(Text name : values) {
                context.write(new Text(name), new Text(key.toString()));
            }
        }
    }

    public static void rankView(String inPath, String outPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Task4-3: rankViewer");
        job.setJarByClass(PageRankViewer.class);

        job.setMapperClass(PageRankViewer.RankViewerMapper.class);
        job.setMapOutputKeyClass(DecDoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(PageRankViewer.RankViewerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
    }
}