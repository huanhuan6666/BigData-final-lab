package pagerank;

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

public class PageRankIter {

    public static class PageRankIterMapper extends Mapper<LongWritable, Text, Text, Text> {
        /*
         * 根据GraphBulider输出的邻接链表结构进行迭代计算
         * Input：key:lineoff value:唐僧\tcur_PR$[悟空, 0.5|八戒, 0.5]
         * Output: 要输出两种键值对
         *  1)用于维护图的邻接链表： key:唐僧  value:[悟空, 0.5|八戒, 0.5]
         *  2)用于输出贡献值：key:唐僧 value:cur_PR*weight
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] temp1 = line.split("\t");
            String[] temp2 = temp1[1].split("\\$");
            String name = temp1[0], graphList = temp2[1];
            double curPR = Double.parseDouble(temp2[0]);
            String[] outPages = graphList.substring(1, graphList.length()-1).split("\\|");
            // 输出邻接链表信息
            context.write(new Text(name), new Text(graphList));

            for(String page : outPages) {
                String[] temp3 = page.split(",");
                if(temp3.length < 2) continue;
                double weight = Double.parseDouble(temp3[1]);
                String contribution = String.valueOf(curPR * weight);
                // 输出PR贡献值信息
                context.write(new Text(temp3[0]), new Text(contribution));
            }
        }
    }

    public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text>
    {
        /*
         * Input为Mapper的输出 key都是lineoff value有两种情况
         * 1) 唐僧\t[悟空, 0.5|八戒, 0.5]
         * 2) 唐僧\tcur_PR*weight
         *
         * Output格式为
         * key:唐僧 value:cur_PR$[悟空, 0.5|八戒, 0.5]
         * 用于下一次迭代
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double curPR = 0;
            String graphList = "";
            for(Text value : values) {
                String item = value.toString();
                if(item.charAt(0) == '[') { // 是邻接链表
                    graphList = item;
                } else { // 是贡献值
                    curPR += Double.parseDouble(item);
                }
            }
            String contribution = String.valueOf(curPR);
            context.write(key, new Text(contribution + "$" + graphList));
        }
    }

    public static void pageRankIter(String inPath, String outPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Task4-2: pageRankIter");
        job.setJarByClass(PageRankIter.class);

        job.setMapperClass(PageRankIter.PageRankIterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(PageRankIter.PageRankIterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.waitForCompletion(true);
    }

}