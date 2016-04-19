package job3;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PairFoodMapper  extends Mapper<LongWritable, Text, BiKeyWritable, BiItemWritable>{
	private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private final BiItemWritable ONE= new BiItemWritable(new Text(""),1);
	private Text data = new Text();
	private LinkedList<String> foodList= new LinkedList<String>();
	
 	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		data.set(line.substring(0,7));
		String idReceipt="receipt"+data.toString()+key.toString();
		ONE.setStringValue(new Text(idReceipt));
		ONE.setIntValue(key.hashCode());
		
		int init=11;
		String a="";
		String food="";
		for(int i=11; i<=line.length();i++){
			if(i==line.length()){
				food=line.substring(init,i);
				insertToList(food);
			}
			if(i<line.length()){
				a=line.substring(i,i+1);
				if(a.equals(",") || i==line.length()){
					food=line.substring(init,i);
					insertToList(food);
					init=i+1;
				}
			}
		}
		readList(context);
	}

	private void readList(Mapper<LongWritable, Text, BiKeyWritable, BiItemWritable>.Context context) {
		//TODO
		//BIKEY.set(new Text(food), new Text(food));
		//context.write(BIKEY, ONE);
		
	}

	private void insertToList(String food) {
		boolean newFood=true;
		for (String f : foodList) {
			if(f.equals(food)) newFood=false;
		}
		if(newFood) foodList.add(food);
	}
}
