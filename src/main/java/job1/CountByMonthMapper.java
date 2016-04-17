package job1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

								//Class Mapper<KEYIN,	   VALUEIN,KEYOUT,VALUEOUT>
public class CountByMonthMapper extends Mapper<LongWritable, Text, BiKeyWritable, IntWritable>  {
	
//	private static final IntWritable one = new IntWritable();
	private static final IntWritable ONE = new IntWritable(1);
    private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private Text data = new Text();
//	private final IntWritable one= new IntWritable(1);
	
 	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		data.set(line.substring(0,7));
		
		int init=11;
		String a="";
		for(int i=11; i<=line.length();i++){
			if(i==line.length()){
//				BIKEY = new BiKeyWritable();
				BIKEY.set(data, new Text(line.substring(init,i)));
				context.write(BIKEY, ONE);
			}
			if(i<line.length()){
				a=line.substring(i,i+1);
				if(a.equals(",") || i==line.length()){
//					BIKEY = new BiKeyWritable();
					BIKEY.set(data, new Text(line.substring(init,i)));
				  	context.write(BIKEY, ONE);
					init=i+1;
				}
			}
		}
	}
}
