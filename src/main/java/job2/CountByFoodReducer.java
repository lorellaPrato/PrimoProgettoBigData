package job2;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CountByFoodReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {

	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {
		
		BiItemWritable ITEM_PRICE= new BiItemWritable(key.getFirst_key(), 0);
		ArrayList<BiItemWritable>cache= new ArrayList<BiItemWritable>();
		
		String res="";
		int price=0;
		int totPrice = 1;
		
		//inserisco gli item in una cache
		for (BiItemWritable item : values) {
			BiItemWritable b= new BiItemWritable();
			b.setIntValue(item.getIntValue());
			b.setStringValue(item.getStringValue());
			cache.add(b);
		}
		
	  //scandisco la cache e cerco il prezzo
		for (int i=0; i<cache.size(); i++) {
			//BiItemWritable val= cache.get(i);
			if(ITEM_PRICE.equals(cache.get(i))){
				price=(cache.get(i)).getIntValue();
			}
		}
		
		//scandisco la cache e creo l'elenco dei result
		boolean first=true;
		if(price>0 && cache.size()>1){
			for (int j=0; j<cache.size(); j++) {
				BiItemWritable val2= cache.get(j);
				if(!(ITEM_PRICE.equals(cache.get(j)))){
					totPrice= price*val2.getIntValue();
					if(first){
						res=res+val2.getStringValue()+":"+totPrice;
						first=false;
					}
					else res=res+", "+val2.getStringValue()+":"+totPrice;
				}
			}
			context.write(key.getFirst_key(), new Text(res));
		}
		
	}
}
