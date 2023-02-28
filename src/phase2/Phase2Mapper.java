package phase2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Phase2Mapper extends Mapper<LongWritable, Text, Phase2Key, IntWritable> {
    Map<String, String> name_tab = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Phase2Key, IntWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        name_tab.put("唐三藏", "唐僧");
        name_tab.put("陈玄奘", "唐僧");
        name_tab.put("玄奘", "唐僧");
        name_tab.put("唐长老", "唐僧");
        name_tab.put("金蝉子", "唐僧");
        name_tab.put("旃檀功德佛", "唐僧");
        name_tab.put("江流儿", "唐僧");
        name_tab.put("江流", "唐僧");

        name_tab.put("悟空", "孙悟空");
        name_tab.put("齐天大圣", "孙悟空");
        name_tab.put("美猴王", "孙悟空");
        name_tab.put("猴王", "孙悟空");
        name_tab.put("斗战胜佛", "孙悟空");
        name_tab.put("孙行者", "孙悟空");
        name_tab.put("心猿", "孙悟空");
        name_tab.put("金公", "孙悟空");

        name_tab.put("猪悟能", "猪八戒");
        name_tab.put("悟能", "猪八戒");
        name_tab.put("八戒", "猪八戒");
        name_tab.put("猪刚鬣", "猪八戒");
        name_tab.put("老猪", "猪八戒");
        name_tab.put("净坛使者", "猪八戒");
        name_tab.put("天蓬元帅", "猪八戒");
        name_tab.put("木母", "猪八戒");

        name_tab.put("沙和尚", "沙僧");
        name_tab.put("沙悟净", "沙僧");
        name_tab.put("悟净", "沙僧");
        name_tab.put("金身罗汉", "沙僧");
        name_tab.put("卷帘大将", "沙僧");
        name_tab.put("刀圭", "沙僧");

        name_tab.put("小白龙", "白龙马");
        name_tab.put("白马", "白龙马");
        name_tab.put("八部天龙马", "白龙马");

        name_tab.put("如来", "如来佛祖");

        name_tab.put("观音", "观音菩萨");
        name_tab.put("观世音菩萨", "观音菩萨");
        name_tab.put("观世音", "观音菩萨");

        name_tab.put("玉帝", "玉皇大帝");
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Phase2Key, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] str_list = value.toString().split(" ");

        Set<String> name_set = new HashSet<>();
        for(String name:str_list) {
            //name_set.add(name_tab.getOrDefault(name, name);
            if(name_tab.containsKey(name))
                name_set.add(name_tab.get(name));
            else
                name_set.add(name);
        }

        int n = name_set.size();
        String[] name_list = name_set.toArray(new String[n]);
        for(int i = 0; i < n; i++) {
            for(int j = i + 1; j < n; j++) {
                context.write(new Phase2Key(new Text(name_list[i]), new Text(name_list[j])), new IntWritable(1));
                context.write(new Phase2Key(new Text(name_list[j]), new Text(name_list[i])), new IntWritable(1));
            }
        }
    }
}