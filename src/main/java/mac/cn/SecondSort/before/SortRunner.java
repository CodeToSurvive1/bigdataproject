package mac.cn.SecondSort.before;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * sort启动类
 *
 * @author lixiaojiao
 * @create 2016-10-22 09:59
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class SortRunner implements Tool {

    private Configuration conf;

    public int run(String[] args) throws Exception {


        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "sort test");


        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/user/mac/mapreduce/sort/input"));
        FileOutputFormat.setOutputPath(job, new Path("/user/mac/mapreduce/sort/output"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }


    public static void main(String args[]) throws Exception {

        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://mac.cn:8020");

        ToolRunner.run(config, new SortRunner(), args);
    }

}
