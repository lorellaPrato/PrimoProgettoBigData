package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StatisticReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {
	private static final BiItemWritable TOT = new BiItemWritable(new Text("tot"), 1);

	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Text food1= key.getFirst_key();
		int tot = 0;
		int grade=0;
		String finalKey= food1.toString();
		String result = "";
		Iterator<BiItemWritable> iter = values.iterator();
		while (iter.hasNext()) {
			BiItemWritable item = iter.next();
			if (item.equals(TOT)) tot = item.getIntValue();
			if ((item.getStringValue()).equals(food1)) grade=item.getIntValue();
		}
		
		if (tot > 0 && grade>0) {
			Iterator<BiItemWritable> it = values.iterator();
			while (it.hasNext()) {
				BiItemWritable second = it.next();
				Text food2= second.getStringValue(); 
				if(!food2.equals(food1) && !second.equals(TOT)){
					finalKey= finalKey+","+food2.toString()+":";
					int perc1= (second.getIntValue())/(tot);
					int perc2= (second.getIntValue())/(grade);
					result=perc1+"% "+perc2+"% ";
					context.write(new Text(finalKey), new Text(result));
				}
			}
		}
		else context.write(food1, new Text("Error"));

	}

}
