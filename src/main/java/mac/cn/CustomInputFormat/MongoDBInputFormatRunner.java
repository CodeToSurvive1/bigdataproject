package mac.cn.CustomInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * MongoDBInputFormat启动类
 *
 * @author lixiaojiao
 * @create 2016-10-17 21:59
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class MongoDBInputFormatRunner {

    static class MongoDBMapper extends Mapper<LongWritable, MongoDBWritable, LongWritable, MongoDBWritable> {
        @Override
        protected void map(LongWritable key, MongoDBWritable value, Context context) throws IOException, InterruptedException {
            if(value.getAge()==null){
                return;
            }
            context.write(new LongWritable(value.getAge()), value);
        }
    }

    static class MongoDBReducer extends Reducer<LongWritable, MongoDBWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<MongoDBWritable> values, Context context) throws IOException, InterruptedException {

            Map<String,Integer> ages = new HashMap<String, Integer>();

            for(MongoDBWritable value : values){
                if(ages.containsKey(value.getAge().toString())){
                    Integer number = ages.get(value.getAge().toString());
                    number++;
                    ages.put(value.getAge().toString(),number);
                }else{
                    ages.put(value.getAge().toString(),1);
                }
            }

            Set<Map.Entry<String, Integer>> entries = ages.entrySet();

            Iterator<Map.Entry<String, Integer>> iterator = entries.iterator();
            while(iterator.hasNext()){
                Map.Entry<String, Integer> next = iterator.next();
                context.write(new IntWritable(Integer.parseInt(next.getKey())),new IntWritable(next.getValue()));
            }

        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration config = new Configuration();
        config.set("fs.defaultFS","hdfs://mac.cn:8020");


        Job job = Job.getInstance(config, "MongoDb input format");

        job.setInputFormatClass(MongoDBInputFormat.class);

        job.setMapperClass(MongoDBMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MongoDBWritable.class);


        job.setReducerClass(MongoDBReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job,new Path("/user/mac/mapreduce/output3"));

        System.exit(job.waitForCompletion(true)?0:1);
    }

}
