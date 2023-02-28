package labelprop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelPropDriver {
    // parameter: 
    //      args[0]: the path of Phase3 output directory
    //      args[1]: maximum iter time
    public static void main(String args[]) throws Exception {
        int MAX = Integer.parseInt(args[1]);
        String forItr[] = { "", "", "" };
        for (int i = 0; i < MAX; i++) {
            forItr[0] = args[0] + "/Data" + (i);
            forItr[1] = args[0] + "/Data" + (i + 1);
            forItr[2] = "" + (i + 1);
            LabelPropIter.main(forItr);
        }

        // finishing, collect the result

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "collect result");// 新建一个用户定义的Job
        job.setJarByClass(LabelPropDriver.class); // 设置执行任务的jar
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LabelPropViewMapper.class); // 设置Mapper类
        job.setReducerClass(LabelPropViewReducer.class); // 设置Reducer类
        job.setOutputKeyClass(Text.class); // 设置job输出的key
        // 设置job输出的value
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NameFloatPair.class);

        // 设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(args[0] + "/Data" + MAX));
        // 设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/result"));
        // 提交任务并等待任务完成
        job.waitForCompletion(true);

        // System.exit(0);

        job = Job.getInstance(conf, "combine result");// 新建一个用户定义的Job
        job.setJarByClass(LabelPropDriver.class); // 设置执行任务的jar
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LabelCombineMapper.class); // 设置Mapper类
        job.setReducerClass(LabelCombineReducer.class); // 设置Reducer类
        job.setOutputKeyClass(Text.class); // 设置job输出的key
        // 设置job输出的value
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(args[0] + "/result"));
        // 设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(args[0] + "/final_result"));
        // 提交任务并等待任务完成
        job.waitForCompletion(true);
    }
}
