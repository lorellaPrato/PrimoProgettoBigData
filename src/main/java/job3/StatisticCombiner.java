package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StatisticCombiner extends Reducer<BiKeyWritable, ItemWritable, BiKeyWritable, ItemWritable>{
	private final static ItemWritable RES = new ItemWritable();
	private final static ItemWritable RES2 = new ItemWritable();
	private static final BiKeyWritable BIKEY = new BiKeyWritable(new Text("unique"),new Text("unique"));
    
    public void reduce(BiKeyWritable key, Iterable<ItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
   
    	 int sum = 0;
         Iterator<ItemWritable> iter = values.iterator();
         while (iter.hasNext()) {
             sum += (iter.next()).getIntValue();
         }
         RES.setStringValue(key.getFirst_key());
         RES.setStringSecondValue(key.getSecond_key());
         RES.setIntValue(sum);
    	 context.write(BIKEY, RES);
    	 if(!(key.getFirst_key()).equals((key.getSecond_key()))){
    		 RES2.setStringValue(key.getSecond_key());
             RES2.setStringSecondValue(key.getFirst_key());
             RES2.setIntValue(sum);
        	 context.write(BIKEY, RES2);
    	 }
    }

}
