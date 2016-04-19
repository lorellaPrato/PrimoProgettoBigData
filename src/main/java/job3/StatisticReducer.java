package job3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StatisticReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {
	private BiItemWritable TOT_REC = new BiItemWritable(new Text("tot_receipts"), 1);

	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {

		int tot = 0;
		int grade=0;
		String result = "";
		Iterator<BiItemWritable> iter = values.iterator();
		while (iter.hasNext()) {
			BiItemWritable item = iter.next();
			if (item.equals(TOT_REC))
				tot = item.getIntValue();
		}
		if (tot > 0) {
			Iterator<BiItemWritable> it_pair = values.iterator();
			while (it_pair.hasNext()) {
				BiItemWritable pair = it_pair.next();
				if ((pair.getStringValue().toString()).contains(",")) {
					grade=(pair.getIntValue())/tot;
					result=grade+"%";
					String food = searchFood(pair.getStringValue().toString());
					Iterator<BiItemWritable> it = values.iterator();
					while (it.hasNext()) {
						BiItemWritable item = it.next();
						if (!(item.getStringValue().toString()).contains(",")) {
							if ((item.getStringValue().toString()).equals(food)) {
								grade=(pair.getIntValue())/(item.getIntValue());
								result=result+" "+grade+"%";
								context.write(new Text(pair.getStringValue().toString()), 
										new Text(result));
							}
						}
					}
				}

			}

		}

	}

	private String searchFood(String string) {
		// TODO Auto-generated method stub
		return null;
	}
}
