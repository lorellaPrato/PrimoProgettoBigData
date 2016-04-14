package input;

import java.util.LinkedList;

public class Receipt {
	private String date;
	private LinkedList<String> foods;
	
	public static void main(String[] args){
		String s="2015-08-28,formaggio,insalata,acqua";
		String date=s.substring(0,10);
		System.out.println(date);
		int init=11;
		String a="";
		for(int i=11; i<=s.length();i++){
			if(i==s.length()){
				System.out.println((s.substring(init,i)));
			}
			if(i<s.length()){
				a=s.substring(i,i+1);
				if(a.equals(",") || i==s.length()){
					System.out.println((s.substring(init,i)));
					init=i+1;
				}
			}
			
		}
	}

	
	public String getYear() {
		if(date.length()>=4) return date.substring(0,4);
		else return null;
	}
	public String getMonth() {
		if(date.length()>=7) return date.substring(5,7);
		else return null;
	}
	public String getDay() {
		if(date.length()>=9) return date.substring(8,10);
		else return null;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	
	public LinkedList<String> getFoods() {
		return foods;
	}
	
	public void setFoods(LinkedList<String> foods) {
		this.foods = foods;
	}
	
	public void addFood (String food){
		this.foods.add(food);
	}
}
