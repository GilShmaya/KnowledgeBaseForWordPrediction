package utils;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class NewProbability implements WritableComparable<NewProbability> {
    private String W1;
    private String W2;
    private Double probability;


    public NewProbability() {
    }

    public NewProbability(String w1, String w2, Double otherProbability) {
        W1 = w1;
        W2 = w2;
        probability = otherProbability;
    }

    @Override
    public int compareTo(NewProbability other) {
        int compareProb = gettersTwoWords().compareTo(other.gettersTwoWords());
        // compare probabilities only if the first two words of two different trigrams is the same
        if (compareProb == 0) {
            if (probability < other.probability)
                return 1;
            else if (probability > other.probability)
                return -1;
            else
                return 0;
        }
        return compareProb;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(W1);
        dataOutput.writeUTF(W2);
        dataOutput.writeDouble(probability);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        W1 = dataInput.readUTF();
        W2 = dataInput.readUTF();
        probability = dataInput.readDouble();
    }

    public String gettersTwoWords() {
        String firstTwo = W1 + " " + W2;
        return firstTwo;
    }

    public double getProbability() {
        return probability;
    }

    public String getProbabilityString() {
        return String.valueOf(probability);
    }

    @Override
    public String toString() {
        String newProbToString = W1 + " " + W2 + " " + String.valueOf(probability);
        return newProbToString;
    }
}
