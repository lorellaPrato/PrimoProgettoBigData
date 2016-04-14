package job1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountByMonthReducer extends Reducer<Text, Writable, Text, Writable> {
//private final static IntWritable SUM = new IntWritable();
    
    public void reduce(Text key, Iterable<Writable> values, Context context) 
    				throws IOException, InterruptedException {
       
    	String elenco = "";
    	Iterator<Writable> it = values.iterator();
    	elenco = it.next().toString();
    	while(it.hasNext()){
    		Writable w = it.next();
    		elenco = ", "+ w.toString();
    	}
    	
    	context.write(key, new Text(elenco));
    }
}
