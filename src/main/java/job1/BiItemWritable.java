package job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BiItemWritable implements WritableComparable<BiItemWritable> {

    private Text word;
    private int count;

    public BiItemWritable() {
    }

    public BiItemWritable(Text w, int c) {
        this.count = c;
        this.word = w;
    }
    
    public static BiItemWritable read(DataInput in) throws IOException {
	BiItemWritable item= new BiItemWritable();
	item.readFields(in);
        return item;
    }

    public void readFields(DataInput in) throws IOException {
        word = new Text(in.readUTF());
        count = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(word.toString());
        out.writeInt(count);
    }

    public void setWord(Text w) {
        this.word = w;
    }
    
    public void setCount(int c) {
        this.count=c;
    }
    
    public Text getWord() {
    	return word;
    }
    
    public int getCount() {
    	return count;
    }

    @Override
    public String toString() {
        return word.toString() + " " + count;
    }

    @Override
    public int hashCode() {
        return count + word.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BiItemWritable) {
        	BiItemWritable c = (BiItemWritable) o;
            return word.equals(c.word);
        }
        return false;
    }

	public int compareTo(BiItemWritable o) {
	 return word.compareTo(o.word);
    }

}
