package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import job1.BiItemWritable;
import job1.BiKeyWritable;
import job1.CountByMonth;
import job1.CountByMonthCombiner;
import job1.CountByMonthMapper;
import job1.CountByMonthReducer;

public class CountByFood {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(new Configuration(), "CountByFood");
		Path p1= new Path(args[0]);
		Path p2= new Path(args[1]);
		job.setJarByClass(CountByFood.class);
		
		
		job.setMapperClass(CountByFoodMapper.class);
		job.setMapperClass(CountByFoodMapperPrice.class);
		// combiner use
		
		job.setCombinerClass(CountByFoodCombiner.class);
		job.setReducerClass(CountByFoodReducer.class);

		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, CountByFoodMapper.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, CountByFoodMapperPrice.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

	
		job.setMapOutputKeyClass(BiKeyWritable.class);
		job.setMapOutputValueClass(BiItemWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		job.waitForCompletion(true);
	}

}
