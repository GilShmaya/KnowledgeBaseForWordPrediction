
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import utils.CounterN;
import utils.NewProbability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import utils.Occurrences;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/***
 * * The DEprobability job (Deleted Estimation probability) is responsible for the following:
 *     1. calculate the Deleted Estimation of each Trigram, according to the formula given in the assignment.
 * 	   2. create an output file that contain : <Trigram> <probability> for each trigram in corpus.
 */


public class DEprobability {

    /***
     * * Map each input line ([w1][w2][w3][Nr][Tr]) into <trigram><Nr,Tr>
     */

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] data = line.toString().split("\\s+");
            if (data.length == 5) {
                String w1 = data[0];
                String w2 = data[1];
                String w3 = data[2];
                String Nr = data[3];
                String Tr = data[4];
                Text key = new Text(w1 + " " + w2 + " " + w3);
                Text value = new Text(Nr + " " + Tr);
                context.write(key, value);
            } else {
                System.out.println("problem in the DEprobability's mapper - incorrect number of words");
            }
        }
    }


    /***
     * * The Reducer gets a trigram as Key and the trigram's Nr1, Tr1, Nr2, Tr2 values as Value.
     * calculate the probability for the given trigram according to the DE method
     */

    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        private MultipleOutputs multiple;
        private double Nr1;
        private double Tr1;
        private double Nr2;
        private double Tr2;
        private int index;
        private double parameterN;
        private String currKey;

        public void setup(Reducer<Text, Text, Text, DoubleWritable>.Context context)
        {
            this.multiple = new MultipleOutputs(context);
            this.index = 0; // indicate whether this the first time we see the trigrm
            this.Nr1 = 1.0D;
            this.Tr1 = 0.0D;
            this.Nr2 = 1.0D;
            this.Tr2 = 0.0D;
            this.parameterN = context.getConfiguration().getLong("N", 1L);
            this.currKey = "";
        }

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            this.currKey = key.toString();
            for (Text value : values) {
                String[] NrTr = value.toString().split("\\s+");
                if (this.index == 0) { // first corpus parts
                    this.index += 1;
                    this.Nr1 = Double.parseDouble(NrTr[0]);
                    this.Tr1 = Float.parseFloat(NrTr[1]);
                }
                else { // second corpus parts
                    this.Nr2 = Double.parseDouble(NrTr[0]);
                    this.Tr2 = Double.parseDouble(NrTr[1]);
                    double DE = (this.Tr1 + this.Tr2) / (this.parameterN * (this.Nr1 + this.Nr2));
                    DoubleWritable de = new DoubleWritable(DE);
                    this.multiple.write("probability", this.currKey, de);
                    index = 0; // get ready to the next trigram
                }
            }
        }

        public void cleanup(Reducer<Text, Text, Text, DoubleWritable>.Context context) {
            try {
                this.multiple.close();
            } catch (IOException|InterruptedException e) {
                System.out.println("Problem in the reduce of trigramSpliter");
                e.printStackTrace();
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        long N = CounterN.getInstance().getN();
        conf.setLong("N", N);
        Job job = Job.getInstance(conf, "DEprobability");
        job.setJarByClass(DEprobability.class);
        job.setMapperClass(DEprobability.MapperClass.class);
        job.setPartitionerClass(DEprobability.PartitionerClass.class);
        job.setReducerClass(DEprobability.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        MultipleInputs.addInputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step3"), TextInputFormat.class,
                DEprobability.MapperClass.class);
        MultipleOutputs.addNamedOutput(job, "probability", TextOutputFormat.class, Text.class, DoubleWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step4"));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
