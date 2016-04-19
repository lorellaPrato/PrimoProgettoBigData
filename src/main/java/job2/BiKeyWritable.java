package job2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class BiKeyWritable implements WritableComparable<BiKeyWritable>{
	private Text first_key;
	private Text second_key;
	
	 public BiKeyWritable() {
	    }

	    public BiKeyWritable(Text k, Text w) {
	        this.first_key = k;
	        this.second_key = w;
	    }
	    
	public void readFields(DataInput in) throws IOException {
		first_key = new Text(in.readUTF());
		second_key = new Text(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(first_key.toString());
        out.writeUTF(second_key.toString());
	}

	@Override
    public String toString() {
        return first_key.toString() + " " + second_key.toString();
    }

    @Override
    public int hashCode() {
        return first_key.hashCode() + second_key.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BiKeyWritable) {
          BiKeyWritable c = (BiKeyWritable) o;
            return first_key.equals(c.first_key)
                    && second_key.equals(c.second_key);
        }
        return false;
    }
		
	public int compareTo(BiKeyWritable o) {
		int cmp = first_key.compareTo(o.first_key);
        if (cmp != 0) {
            return cmp;
        }
        return second_key.compareTo(o.second_key);
	}

	public Text getFirst_key() {
		return first_key;
	}

	public void set(Text first_key,Text second_key) {
		this.first_key = first_key;
		this.second_key = second_key;
	}
	
	public void setFirst_key(Text first_key) {
		this.first_key = first_key;
	}

	public Text getSecond_key() {
		return second_key;
	}

	public void setSecond_key(Text second_key) {
		this.second_key = second_key;
	}
	
}
