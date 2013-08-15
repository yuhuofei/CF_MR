package cf;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.cf.taste.impl.common.FastMap;

public class ItemSimsMapper extends
		Mapper<ItemPairs, IntWritable, ItemPairs, Text> {
	private String path = "";
	private FastMap<IntWritable, Integer> itemNum = new FastMap<IntWritable, Integer>();
	private DecimalFormat df = new DecimalFormat("0.000000");

	public void setup(Context context) throws IOException {
		Configuration conf = new Configuration();

		path = context.getConfiguration().get("item.viewnum");
		FileSystem fs = FileSystem.get(conf);
		Path tempPath = new Path(path + "/job2/");
		SequenceFile.Reader reader = null;
		for (FileStatus stat : fs.listStatus(tempPath)) {
			if (!stat.isDir()) {
				try {
					reader = new SequenceFile.Reader(fs, stat.getPath(), conf);
					Writable key = (Writable) ReflectionUtils.newInstance(
							reader.getKeyClass(), conf);
					Writable value = (Writable) ReflectionUtils.newInstance(
							reader.getValueClass(), conf);
					while (reader.next(key, value)) {
						itemNum.put(
								new IntWritable(
										Integer.parseInt(key.toString())),
								Integer.parseInt(value.toString()));
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					IOUtils.closeStream(reader);
				}
			}
		}

	}

	public void map(ItemPairs key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		float f = (float) 0.0;
		IntWritable first = key.getfirst();
		IntWritable second = key.getsecond();
		int v = Integer.parseInt(value.toString());
		if (v > 0) {
			int joinNum = Integer.parseInt(value.toString());
			f = (float) joinNum
					/ (itemNum.get(first) + itemNum.get(second) - joinNum);
			Text sims = new Text(df.format(f));
			context.write(key, sims);
			context.write(key.swap(), sims);
		}
	}
}