package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatisticReducer extends Reducer<BiKeyWritable, ItemWritable, Text, Text> {

	public void reduce(BiKeyWritable key, Iterable<ItemWritable> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<ItemWritable> cache= new ArrayList<ItemWritable>();
		LinkedList<ItemWritable> list= new LinkedList<ItemWritable>();
		int tot=0;
		int firstFoodValue=0;
		int secondFoodValue=0;
		String finalKey="";
		
		for (ItemWritable item : values) {
			if(item.getStringValue().equals(new Text("scontrino"))){
				tot=item.getIntValue();
			}
			else{
				ItemWritable b= new ItemWritable();
				b.setIntValue(item.getIntValue());	
				b.setStringValue(item.getStringValue());
				b.setStringSecondValue(item.getStringSecondValue());
				if(item.getStringValue().equals(item.getStringSecondValue()))
					list.add(b);
				else 
					cache.add(b);
			}
		}
		
		//scandisco la cache e cerco il prezzo
		for (int i=0; i<cache.size(); i++) {
			ItemWritable val= cache.get(i);
			Text food1=val.getStringValue();
			Text food2=val.getStringSecondValue();
			if(!(food1).equals((food2))){
				firstFoodValue= searchValue(food1,list);
				//secondFoodValue= searchValue(food2);
				double perc1= ((double)(val.getIntValue())/(tot))*100;
				double perc2= ((double)(val.getIntValue())/(firstFoodValue))*100;
				String result=perc1+"% "+perc2+"% ";
				finalKey=val.toString();
				context.write(new Text(finalKey), new Text(result));
			}
		}

}

	private int searchValue(Text stringValue, LinkedList<ItemWritable> list) {
		int value=0;
		for (ItemWritable b : list) {
			if(b.getStringValue().equals(stringValue))
				value=b.getIntValue();
		}
		return value;
	}
}