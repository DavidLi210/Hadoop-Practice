import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWriter implements WritableComparable<PairWriter> {
    @Override
    public int compareTo(PairWriter pairWriter) {
        if(this.from.compareTo(pairWriter.from)==0){
            return this.to.compareTo(pairWriter.to);
        }
        return this.from.compareTo(pairWriter.from);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        from.write(dataOutput);
        to.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        from.readFields(dataInput);
        to.readFields(dataInput);
    }

    public PairWriter() {
        from = new Text();
        to = new Text();
    }

    private Text from;
    private Text to;

/*    public PairWriter(Text from, Text to) {
        this.from = from;
        this.to = to;
    }*/

    @Override
    public boolean equals(Object o) {
        if(o==null){
            return  false;
        }
        if(o.getClass()!=this.getClass()){
            return false;
        }

        if(o==this){
            return true;
        }

        if(o instanceof PairWriter){
            PairWriter other = (PairWriter)o;
            return other.from.equals(this.from)&&other.to.equals(this.to);
        }
        return false;
    }

    public Text getFrom() {
        return from;
    }

    public Text getTo() {
        return to;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((from==null) ? 0:from.hashCode());
        result = prime * result + ((to==null) ? 0:to.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return from.toString()+","+to.toString();
    }
}
