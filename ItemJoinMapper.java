package cf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ItemJoinMapper extends
		Mapper<VarLongWritable, VectorWritable, ItemPairs, IntWritable> {
	private final static IntWritable One = new IntWritable(1);
	//default min and max view num 
	private int minViewNum = 1;
	private int maxViewNum = 100;
	
	public void setup(Context context) throws IOException {
		minViewNum = Integer.parseInt(context.getConfiguration().get("user.minViewNum"));
		maxViewNum = Integer.parseInt(context.getConfiguration().get("user.maxViewNum"));
	}

	public void map(VarLongWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {

		Vector userVector = value.get();
		Iterator<Vector.Element> it = userVector.iterateNonZero();
		IntWritable itemi = new IntWritable();
		ArrayList<Integer> userview = new ArrayList<Integer>();
		while (it.hasNext()) {
			Vector.Element e = it.next();
			int itemIndex = e.index();
			userview.add(itemIndex);
			itemi.set(itemIndex);
			// context.write(key, itemi);
		}
		contextWrite(userview, context);
	}

	public ItemPairs setPairs(ArrayList<Integer> userview, int[] bs) {
		ItemPairs pairs = new ItemPairs();
		boolean firstflag = true;
		for (int kk = 0; kk < bs.length; kk++) {
			if (bs[kk] != 0) {
				if (firstflag) {
					pairs.setfirst(userview.get(kk));
					firstflag = false;
				} else {
					pairs.setsecond(userview.get(kk));
					firstflag = true;
				}
			}
		}
		return pairs;
	}

	public void contextWrite(ArrayList<Integer> userview, Context context)
			throws IOException, InterruptedException {
		int viewnum = userview.size();
		//99% user's view num <100
		if (viewnum > minViewNum && viewnum < maxViewNum) {
			int[] bs = new int[viewnum];
			for (int i = 0; i < 2; i++) {
				bs[i] = 1;
			}
			boolean flag = true;
			boolean tempFlag = false;
			int pos = 0;
			int sum = 0;
			do {
				sum = 0;
				pos = 0;
				tempFlag = true;
				context.write(setPairs(userview, bs), One);
				for (int i = 0; i < viewnum - 1; i++) {
					if (bs[i] == 1 && bs[i + 1] == 0) {
						bs[i] = 0;
						bs[i + 1] = 1;
						pos = i;
						break;
					}
				}
				// 将左边的1全部移动到数组的最左边
				for (int i = 0; i < pos; i++) {
					if (bs[i] == 1) {
						sum++;
					}
				}
				for (int i = 0; i < pos; i++) {
					if (i < sum) {
						bs[i] = 1;
					} else {
						bs[i] = 0;
					}
				}

				// 检查是否所有的1都移动到了最右边
				for (int i = viewnum - 2; i < viewnum; i++) {
					if (bs[i] == 0) {
						tempFlag = false;
						break;
					}
				}
				if (tempFlag == false) {
					flag = true;
				} else {
					flag = false;
				}
			} while (flag);
			if (viewnum > 2) {
				context.write(setPairs(userview, bs), One);
			}
		}
	}
}