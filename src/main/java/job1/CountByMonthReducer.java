package job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountByMonthReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {

	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<BiItemWritable> cache = new ArrayList<BiItemWritable>();

		String result = "";

		// inserisco gli item in una cache
		for (BiItemWritable item : values) {
			BiItemWritable b = new BiItemWritable();
			b.setCount(item.getCount());
			b.setWord(item.getWord());
			cache.add(b);
		}

		// cerco i 5 valori con valori maggiori
		cache = TopFive(cache);

		// scrivo la lista su context
		Iterator<BiItemWritable> iter = cache.iterator();
		result = (iter.next()).toString();
		while (iter.hasNext()) {
			result = result + ", " + (iter.next()).toString();
		}
		context.write(key.getDate(), new Text(result));
	}

	private ArrayList<BiItemWritable> TopFive(ArrayList<BiItemWritable> cache) {
		ArrayList<BiItemWritable> topFiveList = new ArrayList<BiItemWritable>();
		int i=0;
		while(!cache.isEmpty() && i<5){
			BiItemWritable maxItem= new BiItemWritable();
			maxItem= max(cache);
			topFiveList.add(maxItem);
			cache.remove(maxItem);
			i++;
		}
		return topFiveList;
	}

	private BiItemWritable max(ArrayList<BiItemWritable> cache) {
		BiItemWritable maxItem= new BiItemWritable();
		for (BiItemWritable item : cache) {
			if(maxItem.getCount()<item.getCount()) maxItem=item;
		}
		return maxItem;
	}
}
