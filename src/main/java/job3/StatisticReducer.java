package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StatisticReducer extends Reducer<BiKeyWritable, BiItemWritable, BiKeyWritable, BiItemWritable> {
	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {

		String result = "";
		Iterator<BiItemWritable> iter = values.iterator();
		result = (iter.next()).toString();
		while (iter.hasNext()) {
			result = result + ", " + (iter.next()).toString();
		}
		//context.write(key.getDate(), new Text(result));
	}
}
