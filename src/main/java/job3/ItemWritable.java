package job3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ItemWritable implements WritableComparable<ItemWritable> {
	private Text stringFirstValue;
	private Text stringSecondValue;
	private int intValue;

	public ItemWritable() {
    }

	public ItemWritable(Text k1, Text k2, int w) {
        this.stringFirstValue = k1;
        this.stringSecondValue=k2;
        this.intValue = w;
    }
	

	public void readFields(DataInput in) throws IOException {
		stringFirstValue = new Text(in.readUTF());
		stringSecondValue = new Text(in.readUTF());
		intValue =  in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(stringFirstValue.toString());
		out.writeUTF(stringSecondValue.toString());
		out.writeInt(intValue);
	}

	@Override
	public String toString() {
		return stringFirstValue.toString() + "," + stringSecondValue.toString()+": ";
	}

	@Override
	public int hashCode() {
		return stringFirstValue.hashCode()+ stringSecondValue.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ItemWritable) {
			ItemWritable c = (ItemWritable) o;
			return (stringFirstValue.equals(c.stringFirstValue) &&
					stringSecondValue.equals(c.stringSecondValue));
		}
		return false;
	}

	public int compareTo(ItemWritable o) {
		int cmp = stringFirstValue.compareTo(o.stringFirstValue);
        if (cmp != 0) {
            return cmp;
        }
        return stringSecondValue.compareTo(o.stringSecondValue);
	}

	public Text getStringValue() {
		return stringFirstValue;
	}

	public void setStringValue(Text stringValue) {
		this.stringFirstValue = stringValue;
	}

	public int getIntValue() {
		return intValue;
	}

	public void setIntValue(int intValue) {
		this.intValue = intValue;
	}

	public Text getStringSecondValue() {
		return stringSecondValue;
	}

	public void setStringSecondValue(Text stringSecondValue) {
		this.stringSecondValue = stringSecondValue;
	}

}
