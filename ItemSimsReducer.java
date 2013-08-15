package cf;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemSimsReducer extends
		Reducer<ItemPairs, FloatWritable, ItemPairs, FloatWritable> {
	public void reduce(ItemPairs key, Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
	}
}