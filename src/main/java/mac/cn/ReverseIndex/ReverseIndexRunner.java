package mac.cn.ReverseIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by mac on 16/9/27.
 */
public class ReverseIndexRunner implements Tool {

    private Configuration configuration;

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(this.getConf(), "reverseIndex");

        //input
        FileInputFormat.addInputPath(job, new Path("/user/mac/mapreduce/input"));

        //map
        job.setMapperClass(ReverseIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //reduce
        job.setReducerClass(ReverseIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //output
        FileOutputFormat.setOutputPath(job, new Path("/user/mac/mapreduce/output1"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }


    public static void main(String args[]) throws Exception {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://mac.cn:8020");
        ToolRunner.run(config, new ReverseIndexRunner(), args);
    }

}
