package mac.cn.CustomInputFormat;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义MongoDB数据类型
 *
 * @author lixiaojiao
 * @create 2016-10-17 20:55
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class MongoDBWritable implements Writable {

    private String name;

    private Integer age;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        //age为空时不序列化
        if (age == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(age);
        }
    }

    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        if (in.readBoolean()) {
            this.age = in.readInt();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
