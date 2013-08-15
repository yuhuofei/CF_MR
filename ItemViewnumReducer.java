package cf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemViewnumReducer extends
		Reducer<LongWritable, LongWritable, LongWritable, IntWritable> {
	@Override
	public void reduce(LongWritable itemID, Iterable<LongWritable> users,
			Context context) throws IOException, InterruptedException {
		Set<Long> itemusers = new HashSet<Long>();
		for (LongWritable user : users) {
			itemusers.add(Long.parseLong(user.toString()));
		}
		context.write(itemID, new IntWritable(itemusers.size()));
	}
}