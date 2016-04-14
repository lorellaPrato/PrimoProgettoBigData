package input;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public class Data {
	private final String FILENAME="data/lista.txt";
	private static LinkedList<Receipt> receipts=new LinkedList<Receipt>();
	
	public static LinkedList<Receipt> readFile() throws FileNotFoundException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("FILENAME"));
			String s= br.readLine();
			while(s!=null){
				Receipt rec= new Receipt();
				String date=s.substring(0,10);
				rec.setDate(date);
				int init=11;
				String a="";
				for(int i=11; i<=s.length();i++){
					if(i==s.length()){
						rec.addFood(s.substring(init,i));
					}
					if(i<s.length()){
						a=s.substring(i,i+1);
						if(a.equals(",") || i==s.length()){
							rec.addFood(s.substring(init,i));
							init=i+1;
						}
					}
				}
				s= br.readLine();
			}
			br.close();
		} catch (IOException e) {
			System.out.println("Errore apertura file Lista");
		}
		return receipts;	
	}
}
