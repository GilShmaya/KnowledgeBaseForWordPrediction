package utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Aggregator implements WritableComparable<Aggregator> {

    private int corpus_group;
    private long currentR;
    private long otherR;

    public Aggregator(int corpus_group, long currentR, long otherR) {
        this.corpus_group = corpus_group;
        this.currentR = currentR;
        this.otherR = otherR;

    }

    @Override public int compareTo(Aggregator o) {
        return this.toString().compareTo(o.toString());
    }

    @Override public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(corpus_group);
        dataOutput.writeLong(currentR);
        dataOutput.writeLong(otherR);
    }

    @Override public void readFields(DataInput dataInput) throws IOException {
        corpus_group = dataInput.readInt();
        currentR = dataInput.readLong();
        otherR = dataInput.readLong();
    }

    public int getCorpus_group() {
        return corpus_group;
    }

    public void setCorpus_group(int corpus_group) {
        this.corpus_group = corpus_group;
    }

    public long getCurrentR() {
        return currentR;
    }

    public void setCurrentR(long currentR) {
        this.currentR = currentR;
    }

    public long getOtherR() {
        return otherR;
    }

    public void setOtherR(long otherR) {
        this.otherR = otherR;
    }

    public String toString() {
        return corpus_group + " " + currentR + " " + otherR;
    }
}