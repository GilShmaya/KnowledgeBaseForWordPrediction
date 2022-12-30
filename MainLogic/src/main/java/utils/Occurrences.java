package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Occurrences implements WritableComparable<Occurrences> {
    private boolean corpus_group;
    private long count;

    public Occurrences() {
    }

    public Occurrences(boolean corpus, long count) {
        this.corpus_group = corpus;
        this.count = count;
    }

    public int compareTo(Occurrences o) {
        return this.corpusNumericValue() - o.corpusNumericValue();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(this.corpus_group);
        dataOutput.writeLong(this.count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.corpus_group = dataInput.readBoolean();
        this.count = dataInput.readLong();
    }

    public boolean getCorpus_group() {
        return this.corpus_group;
    }

    public int corpusNumericValue() {
        return this.corpus_group ? 0 : 1;
    }

    public long getCount() {
        return this.count;
    }

    public void setCorpus_group(boolean corpus_group) {
        this.corpus_group = corpus_group;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String toString() {
        return this.corpus_group + " " + this.count;
    }
}