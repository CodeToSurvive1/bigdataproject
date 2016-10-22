package mac.cn.SecondSort.after;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 二次排序启动类或者驱动类
 *
 * @author lixiaojiao
 * @create 2016-10-22 12:36
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class SecondSortRunner implements Tool {

    private Configuration config;

    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf,"second sort");


        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(CustomDatatype.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SecondSortReducer.class);
        job.setOutputKeyClass(CustomDatatype.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("/user/mac/mapreduce/sort/input"));
        FileOutputFormat.setOutputPath(job,new Path("/user/mac/mapreduce/sort/secondout"));

        return job.waitForCompletion(true)?0:1;
    }

    public void setConf(Configuration conf) {
        this.config = conf;
    }

    public Configuration getConf() {
        return config;
    }

    public static void main(String args[]) throws Exception {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://mac.cn:8020");
        int exit = ToolRunner.run(config, new SecondSortRunner(),args);
        System.exit(exit);
    }


}
