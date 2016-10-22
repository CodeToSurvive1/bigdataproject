package mac.cn.SecondSort.after;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 二次排序reducer类
 *
 * @author lixiaojiao
 * @create 2016-10-22 12:19
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class SecondSortReducer extends Reducer<CustomDatatype, NullWritable, CustomDatatype, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("-----mapper----start");
    }

    /**
     * 按照注释中的实现逻辑会遗漏掉行记录相同的数据,比如有三行数据相同,那么结果就合并为了一行
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(CustomDatatype key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // context.write(key,NullWritable.get());

        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("-----mapper----end");
    }
}
