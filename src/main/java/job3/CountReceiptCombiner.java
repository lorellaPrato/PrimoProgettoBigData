package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CountReceiptCombiner extends Reducer<BiKeyWritable, BiItemWritable, BiKeyWritable, BiItemWritable>{
	private final static BiItemWritable RES = new BiItemWritable();
    
    public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	 int sum = 0;
         Iterator<BiItemWritable> iter = values.iterator();
         while (iter.hasNext()) {
             sum += (iter.next()).getIntValue();
         }
         RES.setStringValue(new Text("tot"));
         RES.setIntValue(sum);
     
    	 context.write(key, RES);
    }
}
