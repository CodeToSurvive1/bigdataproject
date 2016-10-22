package mac.cn.SecondSort.before;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * sortReducer
 *
 * @author lixiaojiao
 * @create 2016-10-22 09:55
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class SortReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        System.out.println("----reduce---start");
    }

    /**
     * <key,value,value>
     * <key1,value1,value1>
     * 转换为
     * key value
     * key value
     * key1 value1
     * key1 value1
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder valueStr = new StringBuilder();
        for (Text value : values) {
            valueStr.append(value).append(",");
            context.write(key, value);
        }

        System.out.println(key.toString() + "------" + valueStr);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("----reduce---end");
    }
}
