import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.Aggregator;

import java.io.IOException;

/***
 * * The NrTrMaker is responsible for:
 * * 1. Calculate Nri for each R and corpus group i - Nri is the number of n-grams occurring r times in corpus group i
 * * 2. Calculate Tri for each R and corpus group i - Tri  the total number of those n-grams from the part i (those
 * * of Nri) in the 1-i part of the corpus.
 */
public class NrTrCalculator {

    /***
     * * Map each line into <R, Aggregator>. The Aggregator contains the data: corpus group, r1, r2.
     */
    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Aggregator> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] arr = line.toString().split("\\s+"); // "\\s+" is used to match multiple whitespace characters
            if (arr.length == 5) { // <w0, w1, w2, r1, r2>
                long r1 = Long.parseLong(arr[3]);
                long r2 =  Long.parseLong(arr[4]);
                context.write(new LongWritable(r1), new Aggregator(1, 1, r2));
                context.write(new LongWritable(r2), new Aggregator(2, 1, r1));
            } else {
                System.out.println("Error: NrTrMaker job, Mapper - the line should be in the format <w1, w2, w3, r1, r2>");
            }
        }
    }

    /***
     * * Defines the partition policy of sending the key-value the Mapper created to the reducers.
     */
    public static class PartitionerClass extends Partitioner<LongWritable, Aggregator> {
        public int getPartition(LongWritable key, Aggregator value, int numPartitions) {
            //return key.hashCode() % numPartitions;
            return key.hashCode() & Integer.MAX_VALUE % numPartitions;
        }
    }

    /***
     * * Combines the values of Nr1, Nr2, Tr1, Tr2 for each R.
     */
    public static class ReducerClass extends Reducer<LongWritable, Aggregator, LongWritable, Aggregator> {
        private long R;
        private long Nr1;
        private long Nr2;
        private long Tr1;
        private long Tr2;

        public void setup(Context context) {
            R = -1;
            Nr1 = 0;
            Nr2 = 0;
            Tr1 = 0;
            Tr2 = 0;
        }

        public void reduce(LongWritable key, Iterable<Aggregator> values,
                           Context context) throws IOException, InterruptedException {
            for (Aggregator value : values) {
                if (R != key.get()) {
                    R = key.get();
                    Nr1 = 0;
                    Nr2 = 0;
                    Tr1 = 0;
                    Tr2 = 0;
                }
                if (value.getCorpus_group() == 1) {
                    Nr1 += value.getCurrentR();
                    Tr1 += value.getOtherR();
                } else {
                    Nr2 += value.getCurrentR();
                    Tr2 += value.getOtherR();
                }
            }
            context.write(new LongWritable(R), new Aggregator(1, Nr1, Tr1));
            context.write(new LongWritable(R), new Aggregator(2, Nr2, Tr2));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NrTrCalculator");
        job.setJarByClass(NrTrCalculator.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Aggregator.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Aggregator.class);;
        FileInputFormat.addInputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step1"));
        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(MainLogic.BUCKET_PATH + "/Step2"));
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

