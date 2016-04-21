package job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import job2.BiItemWritable;
import job2.BiKeyWritable;

public class CountByFoodMapperPrice extends Mapper<LongWritable, Text, BiKeyWritable, BiItemWritable> {
	private static final BiKeyWritable BIKEY = new BiKeyWritable();
	private static BiItemWritable ITEM = new BiItemWritable(new Text("price"), 1);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		int init = 0;
		int i = 0;
		String line = value.toString();
		String a = "";

		for (i = 0; i <= line.length(); i++) {
			if (a.equals(" ")) {
				String food=line.substring(init, i-1);
				BIKEY.set(new Text(food), new Text(food));
				init = i + 1;
			}
			if (a.equals("$")) {
				int price = Integer.parseInt((line.substring(init - 1, i - 1)));
				ITEM.setIntValue(price);
			}
			if (i < line.length()) {
				a = line.substring(i, i + 1);
			}
		}
		context.write(BIKEY, ITEM);
	}

}
