package mac.cn.ReverseIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by mac on 16/9/27.
 * word : hdfs://user/mac/text1.txt:1 hdfs://user/mac/text1.txt:1 hdfs:hdfs://user/mac/text2.txt:1
 * 将reduce拿到的key,value进行value的合并转化为
 * word : hdfs://user/mac/text1.txt:2 hdfs:hdfs://user/mac/text2.txt:1
 */
public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, Integer> map = new HashMap<String, Integer>();
	private Text valueText = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values) {

			StringBuilder builder = new StringBuilder(value.toString());
			String reverse = builder.reverse().toString();

			//倒转url,进行分片,取number及倒转url
			String[] keyValues = reverse.split(":", 2);

			Integer numberInt = Integer.parseInt(keyValues[0]);
			String keyString = keyValues[1];

			if (map.containsKey(keyString.toString())) {
				Integer oldNumber = map.get(keyString.toString());
				map.put(keyString, oldNumber + numberInt);
			} else {
				map.put(keyString, numberInt);
			}
		}


		//遍历map,循环输出结果
		Set<Map.Entry<String, Integer>> entries =
				map.entrySet();
		Iterator<Map.Entry<String, Integer>> iterator = entries.iterator();
		StringBuilder sb = new StringBuilder();

		while (iterator.hasNext()) {

			Map.Entry<String, Integer> next = iterator.next();

			String key1 = new StringBuilder(next.getKey()).reverse().toString();

			Integer value1 = next.getValue();
			sb.append(key1 + ":" + value1 + ";");
		}


		valueText.set(sb.deleteCharAt(sb.length() - 1).toString());
		context.write(key, valueText);

	}
}
