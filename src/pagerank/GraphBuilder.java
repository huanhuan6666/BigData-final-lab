package pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class GraphBuilder {

    /*
     * 根据Task3的输出构造PageRank所需要的数据结构，即
     * Input：key:lineoff value:唐僧\t[悟空, 0.5|八戒, 0.5]
     * Output: key:唐僧 value:1.0$[悟空, 0.5|八戒, 0.5]
     */
    public static class GraphBuliderMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tmp = value.toString().split("\t");
            String name = tmp[0], graphList = tmp[1], prInit = "1.0$";
            context.write(new Text(name), new Text(prInit + graphList));
        }
    }

    public static void bulidGraph(String inPath, String outPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Task4-1: buildGraph");
        job.setJarByClass(GraphBuilder.class);

        job.setMapperClass(GraphBuliderMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 无需Reducer Mapper结果直接写入文件 格式为 唐僧\t1.0$[悟空, 0.5|八戒, 0.5]
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
    }
}