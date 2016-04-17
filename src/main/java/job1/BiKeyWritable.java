package job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BiKeyWritable implements WritableComparable<BiKeyWritable> {

    private Text date;
    private Text word;

    public BiKeyWritable() {
    }

    public BiKeyWritable(Text k, Text w) {
        this.date = k;
        this.word = w;
    }

    public void readFields(DataInput in) throws IOException {
        date = new Text(in.readUTF());
        word = new Text(in.readUTF());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(date.toString());
        out.writeUTF(word.toString());
    }

    public void setDate(Text k) {
        this.date = k;
    }
    
    public void setWord(Text w) {
        this.word = w;
    }
    
    public void set(Text k,Text w) {
    	this.date = k;
    	this.word = w;
    }
    
    public Text getDate() {
        return date;
    }
    
    public Text getWord() {
    	return word;
    }

    @Override
    public String toString() {
        return date.toString() + " " + word.toString();
    }

    @Override
    public int hashCode() {
        return date.hashCode() + word.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BiKeyWritable) {
          BiKeyWritable c = (BiKeyWritable) o;
            return date.equals(c.date)
                    && word.equals(c.word);
        }
        return false;
    }

    public int compareTo(BiKeyWritable o) {
        int cmp = date.compareTo(o.date);
            if (cmp != 0) {
                return cmp;
            }
            return word.compareTo(o.word);
    }

}