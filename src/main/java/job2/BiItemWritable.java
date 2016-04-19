package job2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BiItemWritable implements WritableComparable<BiItemWritable> {
	private Text stringValue;
	private int intValue;

	public BiItemWritable() {
    }

	public BiItemWritable(Text k, int w) {
        this.stringValue = k;
        this.intValue = w;
    }

	public void readFields(DataInput in) throws IOException {
		stringValue = new Text(in.readUTF());
		intValue =  in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(stringValue.toString());
		out.writeInt(intValue);
	}

	@Override
	public String toString() {
		return stringValue.toString() + " " + intValue;
	}

	@Override
	public int hashCode() {
		return stringValue.hashCode() + intValue;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BiItemWritable) {
			BiItemWritable c = (BiItemWritable) o;
			return stringValue.equals(c.stringValue);
		}
		return false;
	}

	public int compareTo(BiItemWritable o) {
		return stringValue.compareTo(o.stringValue);
	}

	public Text getStringValue() {
		return stringValue;
	}

	public void setStringValue(Text stringValue) {
		this.stringValue = stringValue;
	}

	public int getIntValue() {
		return intValue;
	}

	public void setIntValue(int intValue) {
		this.intValue = intValue;
	}

}
