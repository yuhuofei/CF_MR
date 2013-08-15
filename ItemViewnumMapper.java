package cf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemViewnumMapper extends
		Mapper<LongWritable, Text, LongWritable, LongWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		LongWritable userID = new LongWritable();
		LongWritable itemID = new LongWritable();
		String line = value.toString();
		String[] info = line.split("\t");
		if (info.length < 10) {
			return;
		}
		// change mac to long
		if (info[7].length() == 12 && (!info[8].equals("-1"))) {
			userID.set(Long.parseLong(info[7], 16));
			itemID.set(Long.parseLong(info[8]));
			context.write(itemID, userID);
		}
	}
}
