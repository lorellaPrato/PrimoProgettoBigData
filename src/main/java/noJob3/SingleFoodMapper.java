package noJob3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SingleFoodMapper  extends Mapper<LongWritable, Text, BiKeyWritable, ItemWritable>{
    private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private final ItemWritable ONE= new ItemWritable(new Text(""),1);
	
 	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
//		data.set(line.substring(0,7));
//		String idReceipt="receipt"+data.toString()+key.toString();
//		ONE.setStringValue(new Text(idReceipt));
//		ONE.setIntValue(key.hashCode());
		
		int init=11;
		String a="";
		String food="";
		for(int i=11; i<=line.length();i++){
			if(i==line.length()){
				food=line.substring(init,i);
				BIKEY.set(new Text(food), new Text(food));
				ONE.setStringValue(new Text(food));
				context.write(BIKEY, ONE);
			}
			if(i<line.length()){
				a=line.substring(i,i+1);
				if(a.equals(",") || i==line.length()){
					food=line.substring(init,i);
					BIKEY.set(new Text(food), new Text(food));
					ONE.setStringValue(new Text(food));
				  	context.write(BIKEY, ONE);
					init=i+1;
				}
			}
		}
	}
}
