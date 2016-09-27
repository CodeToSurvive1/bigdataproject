package mac.cn.ReverseIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * 分析文档中的每个单词,将其分析为key->docUrl:number(单词->文档名称:出现次数)的结构
 */
public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text> {

	private String filePath = "";

	private Text wordText = new Text();

	private Text valueText = new Text();

	private StringBuilder valueAppender = new StringBuilder();

	/**
	 * 将文件路径的提取放在setup中,避免放在map中导致重复获取问题
	 *
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		FileSplit filSpilt = (FileSplit) context.getInputSplit();
		filePath = filSpilt.getPath().toString();
	}

	/**
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		valueAppender = new StringBuilder(filePath);
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			String keyString = tokenizer.nextToken();
			wordText.set(keyString);
			valueAppender.append(":1");
			valueText.set(valueAppender.toString());
			context.write(wordText, valueText);
			valueAppender = new StringBuilder(filePath);
		}
	}

}
