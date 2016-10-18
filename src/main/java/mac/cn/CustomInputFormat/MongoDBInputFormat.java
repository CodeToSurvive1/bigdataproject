package mac.cn.CustomInputFormat;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义input format输入格式化器
 *
 * @author lixiaojiao
 * @create 2016-10-17 21:00
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {

    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

        //定义url
        MongoClientURI uri = new MongoClientURI("mongodb://127.0.0.1:27017/hadoop",
                MongoClientOptions.builder().cursorFinalizerEnabled(false));

        //获取连接
        MongoClient client = new MongoClient(uri);

        //获取数据库
        MongoDatabase hadoopDb = client.getDatabase("hadoop");

        //获取集合persons
        MongoCollection<Document> personsCollection = hadoopDb.getCollection("persons");

        //获取总记录数
        long count = personsCollection.count();

        //定义分片的大小为2
        long chunkSize = 2;

        //计算分片数量
        long mappers = count / chunkSize;

        List<InputSplit> splits = new ArrayList<InputSplit>();

        //组装所有的分片位置信息
        for (int i = 0; i < mappers; i++) {
            if (i + 1 == mappers) {
                //如果最后一组数据不恰巧可分,则最后的数据全部放在最后一个mapper中,比如七个数据,最后一组为5,6,7
                splits.add(new MongoDBInputSpilt(i * chunkSize, count));
            } else {
                splits.add(new MongoDBInputSpilt(i * chunkSize, i * chunkSize + chunkSize));
            }
        }

        return splits;
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordReader(split, context);
    }
}
