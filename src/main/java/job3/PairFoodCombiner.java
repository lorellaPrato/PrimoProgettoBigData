package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PairFoodCombiner extends Reducer<BiKeyWritable, BiItemWritable, BiKeyWritable, BiItemWritable>{
	private final static BiItemWritable RES = new BiItemWritable();
    
    public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
    	 String foods=key.getFirst_key().toString()+","+key.getSecond_key().toString();
         Iterator<BiItemWritable> iter = values.iterator();
         int i=1;
         while (iter.hasNext()) {
             i++;
         }
         RES.setIntValue(i);
         RES.setStringValue(new Text(foods));

    	 context.write(key, RES);
    }
}
