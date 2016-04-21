package noJob3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PairFoodCombiner extends Reducer<BiKeyWritable, ItemWritable, BiKeyWritable, ItemWritable>{
	private final static ItemWritable RES = new ItemWritable();
	private final static ItemWritable RES2 = new ItemWritable();
	private final static BiKeyWritable BIKEY = new BiKeyWritable();
	private final static BiKeyWritable BIKEY2 = new BiKeyWritable();
    
    public void reduce(BiKeyWritable key, Iterable<ItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
    	
    	int sum = 0;
        Iterator<ItemWritable> iter = values.iterator();
        while (iter.hasNext()) {
            sum += (iter.next()).getIntValue();
        }
        
        Text food1= key.getFirst_key();
        Text food2= key.getSecond_key();
        
        BIKEY.set(food1, food1);
        RES.setIntValue(sum);
        RES.setStringValue(food2);
        context.write(BIKEY, RES);
        
        BIKEY2.set(food2, food2);
        RES2.setIntValue(sum);
        RES2.setStringValue(food1);
        context.write(BIKEY2, RES2);
    }
}