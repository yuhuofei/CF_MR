package cf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ItemPairs implements WritableComparable<ItemPairs> {
	private IntWritable first;
	private IntWritable second;

	public ItemPairs() {
		set(new IntWritable(), new IntWritable());
	}

	public ItemPairs(String first, String second) {
		set(new IntWritable(Integer.parseInt(first)),
				new IntWritable(Integer.parseInt(second)));
	}

	public ItemPairs(IntWritable first, IntWritable second) {
		set(first, second);
	}

	private void set(IntWritable first, IntWritable second) {
		// TODO Auto-generated method stub
		this.first = first;
		this.second = second;
	}

	public IntWritable getfirst() {
		return first;
	}

	public IntWritable getsecond() {
		return second;
	}

	public void setfirst(int first) {
		this.first = new IntWritable(first);
	}

	public void setsecond(int second) {
		this.second = new IntWritable(second);
	}

	public ItemPairs swap() {
		ItemPairs newPairs = new ItemPairs();
		newPairs.set(this.second, this.first);
		return newPairs;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ItemPairs) {
			ItemPairs kp = (ItemPairs) o;
			return first.equals(kp.getfirst()) && second.equals(kp.getsecond());
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(ItemPairs kp) {
		// TODO Auto-generated method stub
		int cmp = second.compareTo(kp.getsecond());
		if (cmp != 0) {
			return cmp;
		}
		return first.compareTo(kp.getfirst());
	}
}
