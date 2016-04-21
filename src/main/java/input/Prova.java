package input;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;

import job2.BiItemWritable;
import job2.BiKeyWritable;
import job2.CountByFoodReducer;

public class Prova {
	private static LinkedList<BiItemWritable> values= new LinkedList<BiItemWritable>();
	
	public static void main(String[] args) throws Exception {
		reduceJob2Test2();
		//reduceJob2Test();	
	}

	private static void reduceJob3Test() {
		BiItemWritable b1= new BiItemWritable();
		b1.setStringValue(new Text("latte"));
		b1.setIntValue(20);
		values.add(b1);
		BiItemWritable b2= new BiItemWritable();
		b2.setStringValue(new Text("uova"));
		b2.setIntValue(10);
		values.add(b2);
		BiItemWritable b3= new BiItemWritable();
		b3.setStringValue(new Text("dolce"));
		b3.setIntValue(5);
		values.add(b3);
		BiItemWritable b4= new BiItemWritable();
		b4.setStringValue(new Text("tot"));
		b4.setIntValue(100);
		values.add(b4);
		BiItemWritable b5= new BiItemWritable();
		b5.setStringValue(new Text("pane"));
		b5.setIntValue(50);
		values.add(b5);
		
		BiItemWritable TOT = new BiItemWritable(new Text("tot"), 1);
		BiKeyWritable key= new BiKeyWritable(new Text("pane"),new Text("pane"));
		
		Text food1= key.getFirst_key();
		int tot = 0;
		int grade=0;
		String finalKey= food1.toString();
		String result = "";
		Iterator<BiItemWritable> iter = values.iterator();
		while (iter.hasNext()) {
			BiItemWritable item = iter.next();
			if (item.equals(TOT)) tot = item.getIntValue();
			if ((item.getStringValue()).equals(food1)) grade=item.getIntValue();
		}
		
		if (tot > 0 && grade>0) {
			Iterator<BiItemWritable> it = values.iterator();
			while (it.hasNext()) {
				finalKey= food1.toString();
				BiItemWritable second = it.next();
				Text food2= second.getStringValue(); 
				if(!food2.equals(food1) && !second.equals(TOT)){
					finalKey= finalKey+","+food2.toString()+":";
					double perc1= ((double)(second.getIntValue())/(tot))*100;
					double perc2= ((double)(second.getIntValue())/(grade))*100;
					result=perc1+"% "+perc2+"% ";
					//context.write(new Text(finalKey), new Text(result));
					System.out.println(finalKey+" "+result);
				}
			}
		}
		else //context.write(food1, new Text("Error"));
			System.out.println("Error");
	}

	private static void reduceJob2Test() {
		BiItemWritable b1= new BiItemWritable();
		b1.setStringValue(new Text("2015-01"));
		b1.setIntValue(20);
		values.add(b1);
		BiItemWritable b2= new BiItemWritable();
		b2.setStringValue(new Text("2015-02"));
		b2.setIntValue(10);
		values.add(b2);
		BiItemWritable b3= new BiItemWritable();
		b3.setStringValue(new Text("2015-03"));
		b3.setIntValue(5);
		values.add(b3);
		BiItemWritable b4= new BiItemWritable();
		b4.setStringValue(new Text("price"));
		b4.setIntValue(10);
		values.add(b4);
		
		BiItemWritable ITEM_PRICE= new BiItemWritable(new Text("price"), 0);
		BiKeyWritable key= new BiKeyWritable(new Text("pane"),new Text("pane"));
		ArrayList<BiItemWritable>cache= new ArrayList<BiItemWritable>();
		
		String res="";
		int price=0;
		int totPrice = 1;
		int remove=0;
		
		for (BiItemWritable item : values) {
			BiItemWritable b= new BiItemWritable();
			b.setIntValue(item.getIntValue());
			b.setStringValue(item.getStringValue());
			cache.add(item);
		}
		
	  //scandisco la lista e cerco il prezzo
		for (int i=0; i<cache.size(); i++) {
			BiItemWritable val= cache.get(i);
			if(ITEM_PRICE.equals(val)){
				price=val.getIntValue();
				remove=i;
			}
		}
		cache.remove(remove);
		if(price>0){
			for (int j=0; j<cache.size(); j++) {
				BiItemWritable val2= cache.get(j);
				totPrice= price*val2.getIntValue();
				res=res+val2.getStringValue()+":"+totPrice+" ";	
			}
			//context.write(key.getFirst_key(), new Text(res));
			System.out.println(key.getFirst_key()+" "+res);	
		}
	}
	
	private static void reduceJob2Test2() {
		BiItemWritable b1= new BiItemWritable();
		b1.setStringValue(new Text("2015-01"));
		b1.setIntValue(20);
		values.add(b1);
		BiItemWritable b2= new BiItemWritable();
		b2.setStringValue(new Text("2015-02"));
		b2.setIntValue(10);
		values.add(b2);
		BiItemWritable b3= new BiItemWritable();
		b3.setStringValue(new Text("2015-03"));
		b3.setIntValue(5);
		values.add(b3);
		BiItemWritable b4= new BiItemWritable();
		b4.setStringValue(new Text("price"));
		b4.setIntValue(10);
		values.add(b4);
		
		BiItemWritable ITEM_PRICE= new BiItemWritable(new Text("price"), 0);
		BiKeyWritable key= new BiKeyWritable(new Text("pane"),new Text("pane"));
		LinkedList<BiItemWritable>list= new LinkedList<BiItemWritable>();
		
		String res="";
		int price=0;
		int totPrice = 1;
		//scandisco la lista e cerco il prezzo
		for (BiItemWritable item : values) {
			if(ITEM_PRICE.equals(item)) price=item.getIntValue();
			else list.add(item);
		}
		if(!list.isEmpty() && price>0){
			for (BiItemWritable val : list) {
				totPrice= price*val.getIntValue();
				res=res+val.getStringValue()+":"+totPrice+", ";	
			}
			System.out.println(key.getFirst_key()+" "+res);
			//context.write(key.getFirst_key(), new Text(res));
		}
		else {
			if(list.isEmpty()) res="ListaVuota";
			if(!list.isEmpty() && price>0) res="NoPrice";
			System.out.println(key.getFirst_key()+" "+res);
			//context.write(key.getFirst_key(), new Text(res));
		}
	}
}
