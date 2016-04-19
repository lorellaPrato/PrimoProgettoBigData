package job2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import job2.BiItemWritable;
import job2.BiKeyWritable;

public class CountByFoodReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {
	private BiItemWritable ITEM_PRICE= new BiItemWritable(new Text("price"), 1);
	
	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {
		
		String res="";
		int price=0;
		int totPrice = 1;
		//scandisco la lista e cerco il prezzo
		Iterator<BiItemWritable> it = values.iterator();
		while (it.hasNext() && price==0) {
			BiItemWritable item=it.next();
			//if(((item.getStringValue()).toString()).equals("price ")) price=item.getIntValue();
			if(ITEM_PRICE.equals(item)) price=item.getIntValue();
		}
		//ri-scandisco la lista e moltiplico num di pezzi per prezzo
		Iterator<BiItemWritable> it2 = values.iterator();
		if (price>0){	
			while (it2.hasNext()) {
				BiItemWritable item=it2.next();
				//if((item.getStringValue().toString()).equals("price"))
				if(!ITEM_PRICE.equals(item)){
					totPrice= price*item.getIntValue();
					res=item.getStringValue()+": "+totPrice+" ";
					context.write(key.getFirst_key(), new Text(res));
				}
			}
		}
		else{
			res="No_PRICE_FOUND";
			context.write(key.getFirst_key(), new Text(res));
		}
		
		
		
		
	}
}
