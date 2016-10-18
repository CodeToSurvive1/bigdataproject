package mac.cn.CustomInputFormat;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;

/**
 * 自定义MongoDB的RecordReader封装类
 *
 * @author lixiaojiao
 * @create 2016-10-17 21:24
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable, V> {

    private MongoDBInputSpilt inputSpilt;

    private Configuration config;

    private int index = 0;

    private MongoCursor<Document> documentIterator;

    private LongWritable key;

    private V value;

    public MongoDBRecordReader() {
        super();
    }

    public MongoDBRecordReader(InputSplit inputSpilt, TaskAttemptContext context) throws IOException, InterruptedException {
        this.initialize(inputSpilt, context);
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.inputSpilt = (MongoDBInputSpilt) split;
        config = context.getConfiguration();

        this.key = new LongWritable();
        this.value = (V) new MongoDBWritable();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (documentIterator == null) {
            //定义url
            MongoClientURI uri = new MongoClientURI("mongodb://127.0.0.1:27017/hadoop",
                    MongoClientOptions.builder().cursorFinalizerEnabled(false));

            //获取连接
            MongoClient client = new MongoClient(uri);

            //获取数据库
            MongoDatabase hadoopDb = client.getDatabase("hadoop");

            //获取集合persons
            MongoCollection<Document> personsCollection = hadoopDb.getCollection("persons");

            FindIterable<Document> result = personsCollection.find().skip(this.inputSpilt.getStart().intValue()).limit((int) this.inputSpilt.getLength());

            documentIterator = result.iterator();
        }

        if (documentIterator.hasNext()) {
            Document doc = documentIterator.next();

            this.key = new LongWritable(this.inputSpilt.getStart() + index);

            value.setName(doc.get("name").toString());

            if (doc.get("age") != null) {
                value.setAge(Double.valueOf(doc.get("age").toString()).intValue());
            } else {
                value.setAge(null);
            }

            index++;

            return true;
        }

        return false;
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {
        documentIterator.close();
    }
}
