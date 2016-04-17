package job1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountByMonthCombiner extends Reducer<BiKeyWritable, BiItemWritable, BiKeyWritable, BiItemWritable> {
	private final static BiItemWritable RES = new BiItemWritable();
    
    public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	 int sum = 0;
         Iterator<BiItemWritable> iter = values.iterator();
         while (iter.hasNext()) {
             sum += (iter.next()).getCount();
         }
	 RES.setWord(key.getWord());
	 RES.setCount(sum);
         
         key.setWord(new Text(""));
    	 context.write(key, RES);
    }
}

