import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import phase1.*;
import phase2.*;
import phase3.*;
import pagerank.*;
import labelprop.*;

public class MainDriver {
    private static int times = 0;

    // parameter:
    // arg[0]: main class name
    // arg[1]: path of name_list
    // arg[2]: path of data
    // arg[3]: the job to do: "pagerank" or "labelprop"
    // arg[4]: maximum iter time
    public static void main(String[] args) throws Exception {
        assert args.length == 5;
        // 为任务设定配置文件
        Configuration conf = new Configuration();

        // 命令行参数
        // String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf); // 新建一个用户定义的Job

        job.setJarByClass(MainDriver.class); // 设置执行任务的jar

        job.setMapperClass(Phase1Mapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Phase1Reducer.class); // 设置Reducer类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // job.addCacheFile(new
        // Path("/xiyouji/nameList/xiyouji_name_list.txt").toUri()); // 设置分布式缓存文件
        job.addCacheFile(new Path(args[1]).toUri()); // 设置分布式缓存文件

        // Path phase1Input = new Path("/xiyouji/xiyouji_sample");
        Path phase1Input = new Path(args[2]);

        Path phase1Output = new Path("xiyouji/Phase1Output");
        FileInputFormat.setInputPaths(job, phase1Input); // 设置文件输入输出路径
        FileOutputFormat.setOutputPath(job, phase1Output);

        job.waitForCompletion(true);

        // start phase 2
        Configuration configuration = new Configuration();

        job = Job.getInstance(configuration, "Phase2");
        job.setJarByClass(MainDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Phase2Mapper.class);
        job.setReducerClass(Phase2Reducer.class);
        job.setMapOutputKeyClass(Phase2Key.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        Path phase2Output = new Path("xiyouji/Phase2Output");
        FileInputFormat.addInputPath(job, phase1Output);
        FileOutputFormat.setOutputPath(job, phase2Output);

        job.waitForCompletion(true);

        // start phase 3
        job = Job.getInstance(conf, "build graph");// 新建一个用户定义的Job
        job.setJarByClass(MainDriver.class); // 设置执行任务的jar
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Phase3Mapper.class); // 设置Mapper类
        job.setReducerClass(Phase3Reducer.class); // 设置Reducer类
        job.setOutputKeyClass(Text.class); // 设置job输出的key
        // 设置job输出的value
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, phase2Output);
        Path phase3Output = new Path("xiyouji/Phase3Output");
        FileOutputFormat.setOutputPath(job, phase3Output);

        job.waitForCompletion(true);

        if (args[3].equals("pagerank")) {
            // PageRank
            String[] forGB = { "xiyouji/Phase3Output", "xiyouji/Phase4Output/Data0" };
            GraphBuilder.bulidGraph(forGB[0], forGB[1]);

            times = Integer.parseInt(args[4]);
            String[] forItr = { "Data", "Data" };
            for (int i = 0; i < times; i++) {
                forItr[0] = "xiyouji/Phase4Output/Data" + (i);
                forItr[1] = "xiyouji/Phase4Output/Data" + (i + 1);
                PageRankIter.pageRankIter(forItr[0], forItr[1]);
                System.out.println("iter time: " + i);
            }

            String[] forRV = { "xiyouji/Phase4Output/Data" + times, "xiyouji/Phase4Output/FinalRank" };
            PageRankViewer.rankView(forRV[0], forRV[1]);

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } else if (args[3].equals("labelprop")) {
            String[] lpArgs = { "xiyouji/Phase5Output", args[4] };
            LabelPropDriver.main(lpArgs);
        }
    }
}
