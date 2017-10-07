import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairKey implements WritableComparable<PairKey> {
    private Text id = new Text();
    private DoubleWritable rate = new DoubleWritable();
    @Override
    public int compareTo(PairKey pairKey) {
        return pairKey.getRate().compareTo(this.getRate());
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    public DoubleWritable getRate() {
        return rate;
    }

    public void setRate(DoubleWritable rate) {
        this.rate = rate;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        rate.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        rate.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if(o==null) return false;
        else if(o==this) return true;
        if(o.getClass()!=this.getClass()){
            return false;
        }
        if(o instanceof PairKey){
            PairKey key = (PairKey)o;
            if(this.getId().equals(key.getId())&&this.getRate().equals(key.getRate())){
                return true;
            }
            return false;
        }
        return false;
    }

    @Override
    public String toString() {
        return id+"\t"+rate;
    }
}
