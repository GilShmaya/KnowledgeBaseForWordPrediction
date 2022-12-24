
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import utils.NewProbability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;

/***
 * * The SortOutput job is responsible for arranging the output according to the requested order:
 *  (1) by w1w2, ascending; (2) by the probability for w1w2w3, descending.
 */

public class SortOutput {


    /***
     * * Map every line (w1 w2 w3 probability) into <NewProbability, w3>
     *     NewProbability update the probability of the pair w1w2 to be the probability of the trigram w1w2w3 in order
     *     to know which w3 should appear first with w1w2.
     */

    public static class MapperClass extends Mapper<LongWritable, Text, NewProbability, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] data = line.toString().split("\\s+");
            // data[0-2] = w1w2w3, data[3] = probability of w1w2w3
            if (data.length == 4) {
                context.write(new NewProbability(data[0], data[1], Double.parseDouble(data[3])), new Text(data[2]));
            } else {
                System.out.println("problem in the mapper of ArrangingTheResult - incorrect number of words"); // todo
            }
        }
    }

    /***
     * * The Reducer gets the pairs <NewProbability, w3> from the Mapper and creates the final output - key: <w1w2w3> , value: <probability of trigram>.
     */

    public static class ReducerClass extends Reducer<NewProbability,Text,Text, Text> {
        private MultipleOutputs multiple;

        public void setup(Context context){
            multiple= new MultipleOutputs(context);
        }
        public void reduce(NewProbability key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values) {
                multiple.write("Result",new Text(key.firstTwoWords()+" "+value.toString()),new Text(key.getProbabilityString()));
            }
        }
        public void cleanup(Context context)  {
            try {
                multiple.close();
            } catch (IOException | InterruptedException e) {
                System.out.println("Problem in the reduce of trigramSpliter"); // todo
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
        Job job = Job.getInstance(conf, "SortOutput");
        job.setJarByClass(SortOutput.class);

        job.setMapperClass(SortOutput.MapperClass.class);
        job.setPartitionerClass(SortOutput.PartitionerClass.class);
        job.setReducerClass(SortOutput.ReducerClass.class);

        job.setMapOutputKeyClass(NewProbability.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step4"), TextInputFormat.class,
                SortOutput.MapperClass.class);
        MultipleOutputs.addNamedOutput(job,"Result", TextOutputFormat.class,Text.class,Text.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
