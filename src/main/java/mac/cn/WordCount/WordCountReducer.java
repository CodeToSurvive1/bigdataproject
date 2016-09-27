package mac.cn.WordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by mac on 16/9/24.
 */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("reducer setup");
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        Iterator iterator = values.iterator();
        while(iterator.hasNext()){
            LongWritable next = (LongWritable) iterator.next();
            sum+=next.get();
        }

        context.write(key,new LongWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("reducer cleanup");

        super.cleanup(context);
    }
}
