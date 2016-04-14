package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountByMonth {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(new Configuration(), "CountByMonth");

		job.setJarByClass(CountByMonth.class);
		
		job.setMapperClass(CountByMonthMapper.class);
		// combiner use
		//job.setCombinerClass(CountByMonthReducer.class);
		job.setReducerClass(CountByMonthReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Writable.class);

		job.waitForCompletion(true);
	}

}
