package cf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemJoinReducer extends
		Reducer<ItemPairs, IntWritable, ItemPairs, IntWritable> {
	@Override
	public void reduce(ItemPairs userID, Iterable<IntWritable> items,
			Context context) throws IOException, InterruptedException {
		int num = 0;
		for (IntWritable va : items) {
			num += va.get();
		}
		context.write(userID, new IntWritable(num));
	}
}