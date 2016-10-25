package mac.cn.SecondSort.advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 二次排序启动类或者驱动类
 * http://www.superwu.cn/2013/08/18/492/
 * http://blog.csdn.net/w1014074794/article/details/51821727
 * https://www.iteblog.com/archives/1415
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
public class AdvancedSecondSortRunner {

    /**
     * 把第一列整数和第二列作为类的属性，并且实现WritableComparable接口
     */
    public static class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public int hashCode() {
            return first + "".hashCode() + second + "".hashCode();
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }

        //这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first - o.first;
            } else if (second != o.second) {
                return second - o.second;
            } else {
                return 0;
            }
        }


        /** A Comparator that compares serialized IntPair. */
        public static class Comparator extends WritableComparator {
            public Comparator() {
                super(IntPair.class);
            }

            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return compareBytes(b1, s1, l1, b2, s2, l2);
            }
        }

        static {
            WritableComparator.define(IntPair.class, new Comparator());
        }

    }


    public static class SortComparator extends WritableComparator{


        public SortComparator(){
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair o1 = (IntPair) a;
            IntPair o2 = (IntPair) b;

            if (o1.first != o2.first) {
                return o1.first - o2.first;
            } else {
                return o1.second - o2.second;
            }
        }
    }


    /**
     * 在分组比较的时候，只比较原来的key，而不是组合key。
     */
    public static class GroupingComparator implements RawComparator<IntPair> {

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2, s2, Integer.SIZE / 8);
        }

        public int compare(IntPair o1, IntPair o2) {
            int first1 = o1.getFirst();
            int first2 = o2.getFirst();
            return first1 - first2;
        }

    }


    public static class MapClass extends Mapper<LongWritable, Text, IntPair, IntWritable> {

        private final IntPair key = new IntPair();
        private final IntWritable value = new IntWritable();

        @Override
        public void map(LongWritable inKey, Text inValue,
                        Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(inValue.toString());
            int left = 0;
            int right = 0;
            if (itr.hasMoreTokens()) {
                left = Integer.parseInt(itr.nextToken());
                if (itr.hasMoreTokens()) {
                    right = Integer.parseInt(itr.nextToken());
                }
                key.set(left, right);
                value.set(right);
                context.write(key, value);
            }
        }
    }

    public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        private static final Text SEPARATOR = new Text("------------------------------------------------");
        private final Text first = new Text();

        @Override
        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(SEPARATOR, null);
            first.set(Integer.toString(key.getFirst()));
            for (IntWritable value : values) {
                context.write(first, value);
                System.out.println("----");
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://mac.cn:8020");

        Job job = Job.getInstance(conf, "secondary sort");
        job.setJarByClass(AdvancedSecondSortRunner.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setSortComparatorClass(SortComparator.class);

        job.setGroupingComparatorClass(GroupingComparator.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/user/mac/mapreduce/sort/input"));
        FileOutputFormat.setOutputPath(job, new Path("/user/mac/mapreduce/sort/secondout/"));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
