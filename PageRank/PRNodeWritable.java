import org.apache.hadoop.io.*;
import java.io.*;

public class PRNodeWritable implements Writable {

    public static class LongArrayWritable extends ArrayWritable {
        public LongArrayWritable() {
            super(LongWritable.class);
        }
    }

    private LongWritable nid;
    private LongArrayWritable adjList;
    private DoubleWritable pageRank;

    public PRNodeWritable() {
        this.nid = new LongWritable();
        this.adjList = new LongArrayWritable();
        this.pageRank = new DoubleWritable();
    }

    public void setNid(long nid) {
        this.nid.set(nid);
    }
    public long getNid() {
        return this.nid.get();
    }

    public void setAdjList(LongWritable[] values) {
        this.adjList.set(values);
    }
    public LongWritable[] getAdjList() {
        Writable[] ret = this.adjList.get();
        LongWritable[] res = new LongWritable[ret.length];
        for (int i = 0; i < ret.length; i++) {
            res[i] = (LongWritable) ret[i];
        }
        return res;
    }

    public void setPageRank(double pageRank) {
        this.pageRank.set(pageRank);
    }
    public double getPageRank() {
        return this.pageRank.get();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nid.readFields(in);
        this.adjList.readFields(in);
        this.pageRank.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.nid.write(out);
        this.adjList.write(out);
        this.pageRank.write(out);
    }

    @Override
    public String toString() {
        String res = new String();
        for (LongWritable node : this.getAdjList()) {
            res = res + node.toString() + " ";              
        }
        res = res + "," + this.pageRank;
        return res;
    }
}
