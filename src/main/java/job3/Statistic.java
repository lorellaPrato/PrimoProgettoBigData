package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Statistic {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(new Configuration(), "Statistic");
		job.setJarByClass(Statistic.class);
		
//		job.setMapperClass(CountReceiptMapper.class);
//		job.setCombinerClass(CountReceiptCombiner.class);
		
//		job.setMapperClass(SingleFoodMapper.class);
//		job.setCombinerClass(SingleFoodCombiner.class);
		
		job.setMapperClass(PairFoodMapper.class);
		job.setCombinerClass(PairFoodCombiner.class);
		
		
		
//		job.setReducerClass(StatisticReducer.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

	
		job.setMapOutputKeyClass(BiKeyWritable.class);
		job.setMapOutputValueClass(BiItemWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		job.waitForCompletion(true);
	}
}
