package mac.cn.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by mac on 16/9/24.
 */
public class WordCountRunner implements Tool {

    private Configuration config;

    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "wordcount");

        //input
        FileInputFormat.addInputPath(job, new Path("/user/mac/mapreduce/input"));

        //map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        //reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //output
        FileOutputFormat.setOutputPath(job, new Path("/user/mac/mapreduce/output"));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        this.config = conf;
    }

    public Configuration getConf() {
        return config;
    }


    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://mac.cn:8020");
        int run = ToolRunner.run(conf,new WordCountRunner(), args);
        System.exit(run);
    }
}
