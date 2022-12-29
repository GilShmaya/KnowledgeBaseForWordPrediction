package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Aggregator implements WritableComparable<Aggregator> {
    private int corpus_group;
    private long currentR;
    private long otherR;

    public Aggregator() {
    }

    public Aggregator(int corpus_group, long currentR, long otherR) {
        this.corpus_group = corpus_group;
        this.currentR = currentR;
        this.otherR = otherR;
    }

    public int compareTo(Aggregator o) {
        return this.toString().compareTo(o.toString());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.corpus_group);
        dataOutput.writeLong(this.currentR);
        dataOutput.writeLong(this.otherR);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.corpus_group = dataInput.readInt();
        this.currentR = dataInput.readLong();
        this.otherR = dataInput.readLong();
    }

    public int getCorpus_group() {
        return this.corpus_group;
    }

    public void setCorpus_group(int corpus_group) {
        this.corpus_group = corpus_group;
    }

    public long getCurrentR() {
        return this.currentR;
    }

    public void setCurrentR(long currentR) {
        this.currentR = currentR;
    }

    public long getOtherR() {
        return this.otherR;
    }

    public void setOtherR(long otherR) {
        this.otherR = otherR;
    }

    public String toString() {
        return this.corpus_group + " " + this.currentR + " " + this.otherR;
    }
}
