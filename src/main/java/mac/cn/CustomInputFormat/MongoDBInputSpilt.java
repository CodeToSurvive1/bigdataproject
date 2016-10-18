package mac.cn.CustomInputFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义mongodb分片
 *
 * @author lixiaojiao
 * @create 2016-10-17 21:03
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class MongoDBInputSpilt extends InputSplit implements Writable {

    //定义分片起始位置
    private Long start;

    //定义分片终止位置
    private Long end;

    public MongoDBInputSpilt() {
        super();
    }

    public MongoDBInputSpilt(Long start, Long end) {
        super();
        this.start = start;
        this.end = end;
    }

    public long getLength() throws IOException, InterruptedException {
        return end - start;
    }

    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(start);
        out.writeLong(end);
    }

    public void readFields(DataInput in) throws IOException {
        this.start = in.readLong();
        this.end = in.readLong();
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }
}
