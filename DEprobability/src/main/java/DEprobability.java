
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
     * * Input : key - line number , value - the data of that line which contain a trigram and its Nr & Tr values
     ** Output : key - string of the trigram , Value - a string represents the trigram's Nr & Tr values.
     */

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] data = line.toString().split("\\s+");
            if(data.length == 5) {
                String w1 = data[0];
                String w2 = data[1];
                String w3 = data[2];
                String Nr = data[3];
                String Tr = data[4];
                Text key = new Text(w1 + " " + w2 + " " + w3);
                Text value = new Text(Nr + " " + Tr);
                context.write(key, value);

            }
            else {
                System.out.println("problem in the DEprobability's mapper - incorrect number of words");
            }
        }
    }


    /***
     * * The Reducer gets a trigram as Key and the trigram's Nr1, Tr1, Nr2, Tr2 values as Value.
     * calculate the probability for the given trigram according to the DE method
     * @field <index> - indicates on whether we have finished going through all the Nr&Tr values of the trigram
     * @field <ParameterN> - the number of all trigrams in the corpus
     * @field <currKey> - the given trigram as a string (w1w2w3)
     */

    public class ReducerClass extends Reducer<Text,Text,Text, DoubleWritable> {
        private MultipleOutputs multiple;
        private double Nr1;
        private double Tr1;
        private double Nr2;
        private double Tr2;
        private int index;
        private double parameterN;
        private String currKey;

        public void setup(Context context){
            multiple= new MultipleOutputs(context);
            index=0;
            Nr1=1;
            Tr1=0;
            Nr2=1;
            Tr2=0;
            parameterN= (double) context.getConfiguration().getLong("N",1);
            currKey="";
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            currKey = key.toString();
            for (Text value : values) {
                String [] NrTr = value.toString().split("\\s+");
                if(index == 0){
                    index++;
                    Nr1=Double.parseDouble(NrTr[0]);
                    Tr1=Float.parseFloat(NrTr[1]);
                }
                else{
                    Nr2=Double.parseDouble(NrTr[0]);
                    Tr2=Double.parseDouble(NrTr[1]);
                    double DE= ((Tr1+Tr2)/((parameterN)*(Nr1+Nr2)));
                    DoubleWritable de = new DoubleWritable(DE);
                    multiple.write("probability",currKey,de);
                }
            }
        }

        public void cleanup(Context context)  {
            try {
                multiple.close();
            } catch (IOException | InterruptedException e) {
                System.out.println("Problem in the reduce of trigramSpliter");
                e.printStackTrace();
            }
        }
    }

    public static class PartitionerClass extends Partitioner<NewProbability,Text> {
        public int getPartition(NewProbability key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //confNewProbability.setLong("N", Splitter);
        Job job = Job.getInstance(conf, "DEprobability");
        job.setJarByClass(DEprobability.class);
        job.setMapperClass(DEprobability.MapperClass.class);
        job.setPartitionerClass(DEprobability.PartitionerClass.class);
        job.setReducerClass(DEprobability.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        MultipleInputs.addInputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step3"), TextInputFormat.class, DEprobability.MapperClass.class);
        MultipleOutputs.addNamedOutput(job,"probs",TextOutputFormat.class,Text.class,DoubleWritable.class);
        FileOutputFormat.setOutputPath(job,new Path(MainLogic.BUCKET_PATH + "/Step4"));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
