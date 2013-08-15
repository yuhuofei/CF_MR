package cf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ItemsOfUserReducer extends
		Reducer<VarLongWritable, LongWritable, VarLongWritable, VectorWritable> {
	@Override
	public void reduce(VarLongWritable userID, Iterable<LongWritable> items,
			Context context) throws IOException, InterruptedException {
		Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 10);
		int itemNum = 0;
		for (LongWritable item : items) {
			itemNum +=1;
			userVector.set(Integer.parseInt(item.toString()), 1.0);
			if (itemNum > 100) {
				break;
			}
		}
		if (itemNum < 101) {
			context.write(userID, new VectorWritable(userVector));
		}
		/*
		 * Set<String> itemNum = new HashSet<String>(); for(LongWritable
		 * item:items){ itemNum.add(item.toString()); }
		 * 
		 * context.write(userID, new Text(itemNum.toString()));
		 */
	}
}