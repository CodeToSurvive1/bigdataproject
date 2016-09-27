package mac.cn.WordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by mac on 16/9/24.
 */
public class WordCountMapper extends Mapper<Object, Text, Text, LongWritable> {

    private Text keyText = new Text();

    private LongWritable number = new LongWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("mapper setup");
        super.setup(context);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreElements()) {
            String text = (String) tokenizer.nextElement();
            keyText.set(text);
            context.write(keyText, number);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("mapper cleanup");
        super.cleanup(context);
    }
}
