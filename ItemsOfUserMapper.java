package cf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;

public class ItemsOfUserMapper extends
		Mapper<LongWritable, Text, VarLongWritable, LongWritable> {

	private int macPos = 7;
	private int itemPos = 8;

	public void setup(Context context) throws IOException {
		macPos = Integer.parseInt(context.getConfiguration().get("macPos"));
		itemPos = Integer.parseInt(context.getConfiguration().get("itemPos"));
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		VarLongWritable userID = new VarLongWritable();
		LongWritable itemID = new LongWritable();
		String line = value.toString();
		String[] info = line.split("\t");
		if (info.length < max(itemPos, macPos)) {
			return;
		}
		// change mac to long
		if (info[macPos].length() == 12 && (!info[itemPos].equals("-1"))) {
			userID.set(Long.parseLong(info[macPos], 16));
			itemID.set(Long.parseLong(info[itemPos]));
			context.write(userID, itemID);
		}
	}

	private int max(int itemPos2, int macPos2) {
		// TODO Auto-generated method stub
		return itemPos2 > macPos2 ? itemPos2 : macPos2;
	}
}