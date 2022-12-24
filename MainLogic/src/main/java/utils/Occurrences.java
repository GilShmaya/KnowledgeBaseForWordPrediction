package utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Occurrences implements WritableComparable<Occurrences> {
    private boolean corpus_group;
    private long count;

    public Occurrences(){}


    public Occurrences(boolean corpus, long count){
        this.corpus_group = corpus;
        this.count = count;
    }

    @Override public int compareTo(Occurrences o) {
        return corpusNumericValue() - o.corpusNumericValue();
    }

    @Override public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(corpus_group);
        dataOutput.writeLong(count);
    }

    @Override public void readFields(DataInput dataInput) throws IOException {
        corpus_group = dataInput.readBoolean();
        count = dataInput.readLong();
    }

    public boolean getCorpus_group() {
        return corpus_group;
    }

    public int corpusNumericValue(){
        return corpus_group ? 0 : 1;
    }
    public long getCount() {
        return count;
    }

    public void setCorpus_group(boolean corpus_group) {
        this.corpus_group = corpus_group;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String toString(){
        return corpus_group + " " + count;
    }

}
