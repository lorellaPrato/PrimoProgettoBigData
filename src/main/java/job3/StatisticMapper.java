package job3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StatisticMapper extends Mapper<LongWritable, Text, BiKeyWritable, ItemWritable>{
	//private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private final ItemWritable ONE= new ItemWritable(new Text("one"),new Text("one"),1);
	private Text data = new Text();
	
 	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		ArrayList<String> foodList= new ArrayList<String>();
		
		String line = value.toString();
		
		int init=11;
		String a="";
		String food="";
		for(int i=11; i<=line.length();i++){
			if(i==line.length()){
				food=line.substring(init,i);
				foodList.add(food);
			}
			if(i<line.length()){
				a=line.substring(i,i+1);
				if(a.equals(",") || i==line.length()){
					food=line.substring(init,i);
					foodList.add(food);
					init=i+1;
				}
			}
		}
		readList(foodList,context);
	}

	private void readList(ArrayList<String> foodList, Mapper<LongWritable, Text, BiKeyWritable, ItemWritable>.Context context) throws IOException, InterruptedException {
		BiKeyWritable BIKEY = new BiKeyWritable();
		BIKEY.set(new Text("scontrino"),new Text("scontrino"));
		context.write(BIKEY, ONE);
		
		for (String food : foodList) {
			BIKEY = new BiKeyWritable();
			BIKEY.set(new Text(food),new Text(food));
			context.write(BIKEY, ONE);
		}
		
		for(int i=0; i<foodList.size()-1; i++){
			for(int j=i+1; j<foodList.size(); j++){
				BIKEY = new BiKeyWritable();
				if(foodList.get(i).compareTo(foodList.get(j))<=0)
					BIKEY.set(new Text(foodList.get(i)), new Text(foodList.get(j)));
				else if(foodList.get(i).compareTo(foodList.get(j))>0)
					BIKEY.set(new Text(foodList.get(j)), new Text(foodList.get(i)));
				context.write(BIKEY, ONE);
			}
		}
		
	}
}
