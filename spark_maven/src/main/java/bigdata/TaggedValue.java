package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;


import org.apache.hadoop.io.Writable;

public class TaggedValue implements Writable, Serializable {

	public String name;
	public boolean isCity;
	
	public TaggedValue() {
		
	}
	
	public TaggedValue(String name, boolean isCity) {
		this.name = name;
		this.isCity = isCity;
	}

	public void readFields(DataInput in) throws IOException {
		isCity = in.readBoolean();
		name = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isCity);
		out.writeUTF(name);
	}
}