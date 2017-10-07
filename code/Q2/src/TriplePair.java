import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TriplePair {

    private IntWritable count = new IntWritable();
    private Text pw = new Text();
    private Text friends = new Text();

    public Text getFriends() {
        return friends;
    }

    public void setFriends(Text friends) {
        this.friends = friends;
    }

    public IntWritable getCount() {
        return count;
    }

    public Text getPw() {
        return pw;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public void setPw(Text pw) {
        this.pw = pw;
    }
}
