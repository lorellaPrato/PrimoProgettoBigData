package job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CountReceiptMapper  extends Mapper<LongWritable, Text, BiKeyWritable, BiItemWritable>{
	private static final String rec = "tot";
	private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private static final BiItemWritable ONE= new BiItemWritable(new Text(rec),1);
	
	
	
 	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		BIKEY.set(new Text(rec), new Text(rec));
		context.write(BIKEY, ONE);
	}
}
