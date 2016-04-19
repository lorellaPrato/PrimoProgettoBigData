package job2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

import job2.BiItemWritable;
import job2.BiKeyWritable;

public class CountByFoodCombiner extends Reducer<BiKeyWritable, BiItemWritable, BiKeyWritable, BiItemWritable>{
	private final static BiItemWritable RES = new BiItemWritable();
    
    public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	 int sum = 0;
         Iterator<BiItemWritable> iter = values.iterator();
         while (iter.hasNext()) {
             sum += (iter.next()).getIntValue();
         }
         RES.setIntValue(sum);
         RES.setStringValue(key.getSecond_key());
         key.setSecond_key(key.getFirst_key());
    	 context.write(key, RES);
    }
}
