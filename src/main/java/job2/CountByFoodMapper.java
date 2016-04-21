package job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import job2.BiItemWritable;
import job2.BiKeyWritable;

public class CountByFoodMapper extends Mapper<LongWritable, Text, BiKeyWritable, BiItemWritable> {
	private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private Text data = new Text();
	private final BiItemWritable ONE= new BiItemWritable(new Text(""),1);
	
 	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		data.set(line.substring(0,7));
		ONE.setStringValue(data);
		
		int init=11;
		String a="";
		for(int i=11; i<=line.length();i++){
			if(i==line.length()){
				BIKEY.set(new Text(line.substring(init,i)),data);
				context.write(BIKEY, ONE);
			}
			if(i<line.length()){
				a=line.substring(i,i+1);
				if(a.equals(",") || i==line.length()){
					BIKEY.set(new Text(line.substring(init,i)),data);
				  	context.write(BIKEY, ONE);
					init=i+1;
				}
			}
		}
	}
}
