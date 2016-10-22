package mac.cn.SecondSort.after;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 二次排序中自定义数据类型
 *
 * @author lixiaojiao
 * @create 2016-10-22 11:55
 * @email lixiaojiao_hit@163.com
 * @QQ 2305886442
 * @graduate 哈尔滨工业大学
 * @address shanghai china
 * @blog https://CodeToSurvive1.github.io
 * @github https://github.com/CodeToSurvive1
 */
public class CustomDatatype implements WritableComparable<CustomDatatype> {

    private String columnA;

    private String columnB;

    public int compareTo(CustomDatatype o) {
        int result = this.getColumnA().compareTo(o.getColumnA());
        if (result == 0) {
            return this.getColumnB().compareTo(o.getColumnB());
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(columnA);
        out.writeUTF(columnB);
    }

    public void readFields(DataInput in) throws IOException {
        this.columnA = in.readUTF();
        this.columnB = in.readUTF();
    }

    @Override
    public String toString() {
        return this.columnA+" "+this.columnB;
    }

    public String getColumnA() {
        return columnA;
    }

    public void setColumnA(String columnA) {
        this.columnA = columnA;
    }

    public String getColumnB() {
        return columnB;
    }

    public void setColumnB(String columnB) {
        this.columnB = columnB;
    }
}
