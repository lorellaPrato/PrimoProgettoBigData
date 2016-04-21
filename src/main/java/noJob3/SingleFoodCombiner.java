package noJob3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class SingleFoodCombiner extends Reducer<BiKeyWritable, ItemWritable, BiKeyWritable, ItemWritable>{
	private final static ItemWritable RES = new ItemWritable();
    
    public void reduce(BiKeyWritable key, Iterable<ItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	int sum = 0;
        Iterator<ItemWritable> iter = values.iterator();
        while (iter.hasNext()) {
            sum += (iter.next()).getIntValue();
        }
        
         RES.setIntValue(sum);
         RES.setStringValue(key.getFirst_key());
         //RES.setStringValue(new Text(list));

    	 context.write(key, RES);
    }
}
