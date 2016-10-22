package mac.cn.SecondSort.after;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 二次排序mapper类
 *
 * @author lixiaojiao
 * @create 2016-10-22 12:04
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class SecondSortMapper extends Mapper<LongWritable, Text, CustomDatatype, NullWritable> {

    private CustomDatatype customDatatype = new CustomDatatype();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("-----mapper-----start");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] valueLine = value.toString().split(" ");
        String columnA = valueLine[0];
        String columnB = valueLine[1];

        customDatatype.setColumnA(columnA);
        customDatatype.setColumnB(columnB);
        context.write(customDatatype,NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("-----mapper-----end");
    }
}
