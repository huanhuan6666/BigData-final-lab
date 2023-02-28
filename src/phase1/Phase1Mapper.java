package phase1;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

public class Phase1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashSet<String> nameSet = new HashSet<String>(); // 将姓名存入hash表

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取分布式缓存文件列表
        URI[] cacheFiles = context.getCacheFiles();
        // 获取指定分布式缓存文件的文件系统
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
        // 获取文件输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        // 读取文件内容到nameSet
        String line = null;
        while (null != (line = bufferedReader.readLine())) {
            nameSet.add(line);
            DicLibrary.insert(DicLibrary.DEFAULT, line); // 自定义人名字典用于分词
        }

        // 释放资源
        bufferedReader.close();
    }


    //
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 使用ansj_seg对段落分析提取人名
        String line = value.toString();
        Result result = DicAnalysis.parse(line);
        List<Term> names = result.getTerms();
        // 本段落出现的人名写入一行
        StringBuilder stringBuilder = new StringBuilder();
        for (Term name : names) {
            if (nameSet.contains(name.getName())) { // 是想要的人名
                stringBuilder.append(name.getName() + " ");
            }
        }
        // 一段至少有2个人名才输出
        int nameCount = stringBuilder.length();
        if (nameCount >= 2) {
            context.write(new Text(stringBuilder.toString().substring(0, nameCount-1)), new IntWritable(1));
        }
    }
}
