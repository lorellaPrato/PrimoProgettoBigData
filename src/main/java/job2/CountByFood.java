package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import job2.BiItemWritable;
import job2.BiKeyWritable;

public class CountByFood {
	public static void main(String[] args) throws Exception {
		
		Job job = new Job(new Configuration(), "CountByFood");
		Path p1= new Path(args[0]);
		Path p2= new Path(args[1]);
		job.setJarByClass(CountByFood.class);
		
		
		job.setMapperClass(CountByFoodMapper.class);
		job.setCombinerClass(CountByFoodCombiner.class);
		
		job.setMapperClass(CountByFoodMapperPrice.class);
		
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
