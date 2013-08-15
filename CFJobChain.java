package cf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

public class CFJobChain {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException ,FileNotFoundException{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: CFJob <conffile>");
			System.err.println("Example: CFJob /tmp/mr.conf");
			System.exit(2);
		}

		InputStream inputStream;
		inputStream = new FileInputStream(new File(args[0]));
		Properties p = new Properties();
		p.load(inputStream);
			// System.out.println("ip:" + p.getProperty("ip") + ",port:"
			// + p.getProperty("port"));
		/*read conf */
		String preInput1 = p.getProperty("hdfs_indir1");
		String preInput2 = p.getProperty("hdfs_indir2");
		String Output = p.getProperty("hdfs_outdir");
		int day = Integer.parseInt(p.getProperty("dayNum"));
		
		int job1ReduceNum = Integer.parseInt(p.getProperty("job1ReduceNum"));
		int job2ReduceNum = Integer.parseInt(p.getProperty("job2ReduceNum"));
		int job3ReduceNum = Integer.parseInt(p.getProperty("job3ReduceNum"));
		
		String minViewNum = p.getProperty("minViewNum");
		String maxViewNum = p.getProperty("maxViewNum");
		
		String macPos = p.getProperty("macPos");
		String itemPos = p.getProperty("itemPos");
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
		String inputPath = "";
		Calendar cal = Calendar.getInstance();
		for (int i = 1; i < day + 1; i++) {
			cal.add(Calendar.DATE, -1);
			inputPath += (preInput1 + format.format(cal.getTime()) + ",");
		}
		cal = Calendar.getInstance();
		for (int i = 1; i < day + 1; i++) {
			cal.add(Calendar.DATE, -1);
			inputPath += (preInput2 + format.format(cal.getTime()));
			if (i != day) {
				inputPath += ",";
			}
		}

		String tmpdir = "/tmp/" + String.valueOf(System.currentTimeMillis());

		FileSystem fs = FileSystem.get(conf);
		/*result output dir*/
		if (fs.exists(new Path(Output)))
		{
			fs.delete(new Path(Output), true);
		}
		
		/* tmp output dir*/
		while (fs.exists(new Path(tmpdir))) {
			tmpdir = "/tmp/" + String.valueOf(System.currentTimeMillis());
		}

		Path tmppath = new Path(tmpdir);
		conf.set("item.viewnum", tmpdir);
		conf.set("user.minViewNum", minViewNum);
		conf.set("user.maxViewNum", maxViewNum);
		conf.set("macPos", macPos);
		conf.set("itemPos", itemPos);

		/* CF first job; get items_of_user */
		Job job1 = new Job(conf, "CF job one");
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		job1.setNumReduceTasks(job1ReduceNum);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setJarByClass(CFJobChain.class);
		job1.setMapperClass(ItemsOfUserMapper.class);
		job1.setMapOutputKeyClass(VarLongWritable.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setReducerClass(ItemsOfUserReducer.class);
		job1.setOutputKeyClass(VarLongWritable.class);
		job1.setOutputValueClass(VectorWritable.class);

		FileInputFormat.addInputPaths(job1, inputPath);
		SequenceFileOutputFormat.setOutputPath(job1,
				new Path(tmppath + "/job1"));

		/* CF second job; get item view nums */
		Job job2 = new Job(conf, "CF job two");
		job2.setNumReduceTasks(job2ReduceNum);
		job2.setJarByClass(CFJobChain.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setMapperClass(ItemViewnumMapper.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setReducerClass(ItemViewnumReducer.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPaths(job2, inputPath);
		SequenceFileOutputFormat.setOutputPath(job2,
				new Path(tmppath + "/job2"));

		/* CF third job; get item join num */
		Job job3 = new Job(conf, "CF Job three");
		job3.setJarByClass(CFJobChain.class);

		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);

		job3.setMapperClass(ItemJoinMapper.class);
		job3.setReducerClass(ItemJoinReducer.class);
		job3.setCombinerClass(ItemJoinReducer.class);

		job3.setMapOutputKeyClass(ItemPairs.class);
		job3.setMapOutputValueClass(IntWritable.class);

		job3.setOutputKeyClass(ItemPairs.class);
		job3.setOutputValueClass(IntWritable.class);

		job3.setNumReduceTasks(job3ReduceNum);
		SequenceFileInputFormat.addInputPath(job3, new Path(tmppath + "/job1"));
		SequenceFileOutputFormat.setOutputPath(job3,
				new Path(tmppath + "/job3"));

		/* CF forth job; get item sims */
		Job job4 = new Job(conf, "CF job four");
		job4.setNumReduceTasks(1);
		job4.setJarByClass(CFJobChain.class);
		job4.setInputFormatClass(SequenceFileInputFormat.class);
		job4.setMapperClass(ItemSimsMapper.class);
		job4.setMapOutputKeyClass(ItemPairs.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setReducerClass(ItemSimsReducer.class);
		job4.setOutputKeyClass(ItemPairs.class);
		job4.setOutputValueClass(Text.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		job4.setNumReduceTasks(0);
		SequenceFileInputFormat.addInputPath(job4, new Path(tmppath + "/job3"));
		FileOutputFormat.setOutputPath(job4, new Path(Output));

		ControlledJob ctrl_job1 = new ControlledJob(job1.getConfiguration());
		ctrl_job1.setJob(job1);
		JobControl jbcntrl = new JobControl("cfjob");
		jbcntrl.addJob(ctrl_job1);

		ControlledJob ctrl_job2 = new ControlledJob(job2.getConfiguration());
		ctrl_job2.setJob(job2);
		jbcntrl.addJob(ctrl_job2);

		ControlledJob ctrl_job3 = new ControlledJob(job3.getConfiguration());
		ctrl_job3.setJob(job3);
		jbcntrl.addJob(ctrl_job3);
		ctrl_job3.addDependingJob(ctrl_job1);

		ControlledJob ctrl_job4 = new ControlledJob(job4.getConfiguration());
		ctrl_job4.setJob(job4);
		jbcntrl.addJob(ctrl_job4);

		ctrl_job4.addDependingJob(ctrl_job2);
		ctrl_job4.addDependingJob(ctrl_job3);

		Thread jcThread = new Thread(jbcntrl);
		jcThread.start();
		while (true) {
			if (jbcntrl.allFinished()) {
				// System.out.println(jbcntrl.getSuccessfulJobList());
				jbcntrl.stop();
				fs.delete(tmppath, true);
				fs.close();
				System.exit(0);
			}
			if (jbcntrl.getFailedJobList().size() > 0) {
				// System.out.println(jbcntrl.getFailedJobList());
				jbcntrl.stop();
				fs.delete(tmppath, true);
				fs.close();
				System.exit(1);
			}
		}
	}
}
