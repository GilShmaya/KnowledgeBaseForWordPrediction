import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import utils.Aggregator;

import java.io.IOException;

/***
 * * Join the data arrived from the Splitter and the NrTrCalculator jobs
 * Splitter's Output - <trigram, r1, r2>
 * NrTrCalculator's Output - <R, Nr, Tr>
 */
public class Joiner {

    /***
     * * In case the line came from the first Splitter:
     * 		key - R
     * 		Value - <corpus_group, w0, w1, w2>
     * 	 In case the line came from the first NrTrCalculator:
     * 		key - R
     * 		Value - <corpus, Nr, Tr>
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            String[] arr = line.toString().split("\\s+");
            if (arr.length == 4) {
                String R = arr[0];
                String corpus_group = arr[1];
                String Nri = arr[2];
                String Tri = arr[3];
                context.write(new Text(R + "A"), new Text(String.format("%s %s %s", corpus_group, Nri, Tri)));
            }
            else if (arr.length == 5) {
                String w0 = arr[0];
                String w1 = arr[1];
                String w2 = arr[2];
                String r1 = arr[3];
                String r2 = arr[4];
                context.write(new Text(r1 + "B"), new Text(String.format("1 %s %s %s", w0, w1, w2)));
                context.write(new Text(r2 + "B"), new Text(String.format("2 %s %s %s", w0, w1, w2)));
            } else {
                System.out.println("Error: Joiner job, Mapper - invalid input");
            }
        }
    }

    /***
     * * Defines the partition policy of sending the key-value the Mapper created to the reducers.
     */
    public static class PartitionerClass extends Partitioner<LongWritable, Aggregator> {
        public int getPartition(LongWritable key, Aggregator value, int numPartitions) {
                String RInKey = key.toString().substring(0, key.toString().length() - 1);
                return (RInKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
    }

    /**
     * * Combine into 2 lines to each trigram. each line contain : <trigram, Nri, Tri>
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private String R;
        private String NrTr1;
        private String NrTr2;

        public void setup(Context context) {
            R = "";
            NrTr1 = "0 0";
            NrTr2 = "0 0";
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] arr = value.toString().split("\\s+");
                String RInKey = key.toString().substring(0, key.toString().length() - 1);
                if (!this.R.equals(RInKey)) {
                    this.R = RInKey;
                    this.NrTr1 = "0 0";
                    this.NrTr2 = "0 0";
                }

                int len = arr.length;
                if (len == 3) {
                    String corpus_group = arr[0];
                    String Nri = arr[1];
                    String Tri = arr[2];
                    if (Integer.parseInt(corpus_group) == 1)
                        this.NrTr1 = (Nri + " " + Tri);
                    else {
                        this.NrTr2 = (Nri + " " + Tri);
                    }
                }
                if (len == 4) {
                    String corpus_group = arr[0];
                    String w0 = arr[1];
                    String w1 = arr[2];
                    String w2 = arr[3];
                    if (Integer.parseInt(corpus_group) == 1)
                        context.write(new Text(w0 + " " + w1 + " " + w2), new Text(this.NrTr1));
                    else
                        context.write(new Text(w0 + " " + w1 + " " + w2), new Text(this.NrTr2));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Joiner");
        job.setJarByClass(Joiner.class);
        job.setMapperClass(Joiner.MapperClass.class);
        job.setPartitionerClass(Joiner.PartitionerClass.class);
        job.setReducerClass(Joiner.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step1"), TextInputFormat.class, Joiner.MapperClass.class);

        MultipleInputs.addInputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step2"), TextInputFormat.class, Joiner.MapperClass.class);

        FileOutputFormat.setOutputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step3"));
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
