package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class CountByFoodReducer extends Reducer<BiKeyWritable, BiItemWritable, Text, Text> {

	public void reduce(BiKeyWritable key, Iterable<BiItemWritable> values, Context context)
			throws IOException, InterruptedException {
		
		BiItemWritable ITEM_PRICE= new BiItemWritable(key.getFirst_key(), 0);
		ArrayList<BiItemWritable>cache= new ArrayList<BiItemWritable>();
		
		String res="";
		int price=0;
		int totPrice = 1;
		int remove=0;
		
		for (BiItemWritable item : values) {
			BiItemWritable b= new BiItemWritable();
			b.setIntValue(item.getIntValue());
			b.setStringValue(item.getStringValue());
			cache.add(b);
		}
		
	  //scandisco la lista e cerco il prezzo
		for (int i=0; i<cache.size(); i++) {
			//BiItemWritable val= cache.get(i);
			if(ITEM_PRICE.equals(cache.get(i))){
				price=(cache.get(i)).getIntValue();
			}
		}
		//context.write(key.getFirst_key(), new Text("prezzo:"+price+" size:"+cache.size()));
//		cache.remove(remove);
		if(price>0 && cache.size()>1){
			for (int j=0; j<cache.size(); j++) {
				BiItemWritable val2= cache.get(j);
				if(!(ITEM_PRICE.equals(cache.get(j)))){
					totPrice= price*val2.getIntValue();
					res=res+val2.getStringValue()+":"+totPrice+" ";	
				}
			}
			context.write(key.getFirst_key(), new Text(res));
		}
		
//		Iterator<BiItemWritable> it = values.iterator();
//		BiItemWritable item=it.next();
//		price=item.getIntValue();
//		while (it.hasNext()) {
//			item=it.next();
//			//if(((item.getStringValue()).toString()).equals("price ")) price=item.getIntValue();
//			//if(ITEM_PRICE.equals(item)) price=item.getIntValue();
//			totPrice= price*item.getIntValue();
//			res=res+item.getStringValue()+":"+totPrice+", ";	
//		}
//		context.write(key.getFirst_key(), new Text(res));
//		Iterator<BiItemWritable> it2 = values.iterator();
//		//ri-scandisco la lista e moltiplico num di pezzi per prezzo
//		//if (price>0){	
//			while (it2.hasNext()) {
//				BiItemWritable food=it2.next();
//				//if(!(food.getStringValue().toString()).equals(key.getFirst_key())){
//				if(!(food.equals(ITEM_PRICE))){
//					totPrice= price*food.getIntValue();
//					res=res+food.getStringValue()+":"+totPrice+", ";
//				}
//			}
//			context.write(key.getFirst_key(), new Text(res));
//		}
//		else{
//			res="No_PRICE_FOUND";
//			context.write(key.getFirst_key(), new Text(res));
//		}
		
		
		
		
	}

	private int orderList(Iterable<BiItemWritable> values, BiKeyWritable key) {
		BiItemWritable ITEM_PRICE= new BiItemWritable(key.getFirst_key(), 0);
		int price=0;
		Iterator<BiItemWritable> it = values.iterator();
		while (it.hasNext() && price==0) {
			BiItemWritable item=it.next();
			//if(((item.getStringValue()).toString()).equals("price ")) price=item.getIntValue();
			if(ITEM_PRICE.equals(item)) price=item.getIntValue();
		}
		return price;
	}
}
