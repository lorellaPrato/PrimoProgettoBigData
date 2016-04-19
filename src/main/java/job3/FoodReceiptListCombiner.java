package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class FoodReceiptListCombiner extends Reducer<BiKeyWritable, BiItemWritable, BiKeyWritable, BiItemWritable>{
	private final static BiItemWritable RES = new BiItemWritable();
    
    public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
         Iterator<BiItemWritable> iter = values.iterator();
         BiItemWritable item=iter.next();
         int i=1;
         String list="receiptsList:"+(item.getIntValue());
         while (iter.hasNext()) {
             list = list+" "+(iter.next()).getIntValue();
             i++;
         }
         RES.setIntValue(i);
         RES.setStringValue(new Text(list));

    	 context.write(key, RES);
    }
}
