package noJob3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StatisticReducer extends Reducer<BiKeyWritable, ItemWritable, Text, Text> {
	private static final ItemWritable TOT = new ItemWritable(new Text("tot"), 1);

	public void reduce(BiKeyWritable key, Iterable<ItemWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Text food1= key.getFirst_key();
		int tot = 0;
		int grade=0;
		String finalKey= food1.toString();
		String result = "";
		Iterator<ItemWritable> iter = values.iterator();
		while (iter.hasNext()) {
			ItemWritable item = iter.next();
			if (item.equals(TOT)) tot = item.getIntValue();
			if ((item.getStringValue()).equals(food1)) grade=item.getIntValue();
		}
		
		if (tot > 0 && grade>0) {
			Iterator<ItemWritable> it = values.iterator();
			while (it.hasNext()) {
				finalKey= food1.toString();
				ItemWritable second = it.next();
				Text food2= second.getStringValue(); 
				if(!food2.equals(food1) && !second.equals(TOT)){
					finalKey= finalKey+","+food2.toString()+":";
					double perc1= ((double)(second.getIntValue())/(tot))*100;
					double perc2= ((double)(second.getIntValue())/(grade))*100;
					result=perc1+"% "+perc2+"% ";
					context.write(new Text(finalKey), new Text(result));
					//System.out.println(finalKey+" "+result);
				}
			}
		}
		else context.write(food1, new Text("Error"));
			//System.out.println("Error");
	}

}