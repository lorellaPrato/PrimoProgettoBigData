package job1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountByMonthReducer extends Reducer<BiKeyWritable, IntWritable, Text, BiItemWritable> {
	//private final static IntWritable SUM = new IntWritable();
    
    public void reduce(BiKeyWritable key, Iterable<IntWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	 int sum = 0;
         Iterator<IntWritable> iter = values.iterator();
         while (iter.hasNext()) {
             sum += iter.next().get();
         }
         BiItemWritable item= new BiItemWritable(key.getWord(), sum);
    	 context.write(key.getDate(), item);
    }
}
