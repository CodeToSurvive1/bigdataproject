package mac.cn.SecondSort.before;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 排序mapper
 *
 * @author lixiaojiao
 * @create 2016-10-22 09:52
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("----mapper---start");
    }

    /**
     * 输入为1->key    value
     * 35->key    value
     * 转换为key1 value1
     * key2  value2
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(key.toString() + "---->" + value.toString());
        String[] values = value.toString().split(" ");
        String valueKey = values[0];
        String valueValue = values[1];

        context.write(new Text(valueKey), new Text(valueValue));
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("----mapper---end");
    }
}
