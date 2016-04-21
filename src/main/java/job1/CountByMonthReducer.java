package job1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountByMonthReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {
    
    public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	 String result = "";
         Iterator<BiItemWritable> iter = values.iterator();
	 result= (iter.next()).toString();
         while (iter.hasNext()) {
		result= result+", "+(iter.next()).toString();
         }
    	 context.write(key.getDate(), new Text(result));
    }
}

