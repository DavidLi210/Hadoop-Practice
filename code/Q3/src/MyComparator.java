import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {
    public MyComparator() {
        super(PairKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PairKey k1 = (PairKey)a;
        PairKey k2 = (PairKey)b;
        return k1.getId().compareTo(k2.getId());
    }
}
