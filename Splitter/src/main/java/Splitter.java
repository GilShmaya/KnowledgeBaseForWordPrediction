import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import utils.CounterN;
import utils.Occurrences;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/***
 * * The Splitter job is responsible for the following:
 *     1. Divide the corpus.
 *     2. Count the occurrences and calculating N (N is number of word sequences of size 3 in the corpus).
 *     3. Calculate R, the number the trigram occur in the two parts of the corpus for each trigram.
 */
public class Splitter {

    public static long N;

    /***
     * * Map every line into <trigram, Occurrences>, the Occurrences include a division of the corpus into two parts and
     *        the occurrences of every trigram.
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Occurrences> {
        private static final Pattern ENGLISH = Pattern.compile("(?<trigram>[A-Z]+ [A-Z]+ [A-Z]+)\\t\\d{4}\\t" +
                "(?<occurrences>\\d+).*"); // The Ngrams row contains: <n-gram year occurrences pages books>
        // seperated by tap

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            Matcher matcher = ENGLISH.matcher(line.toString());
            if (matcher.matches()) {
                context.write(new Text(matcher.group("trigram")), new Occurrences((lineId.get() % 2 == 0),
                        Long.parseLong(matcher.group("occurrences"))));
            }
        }
    }

    /***
     * * Defines the partition policy of sending the key-value the Mapper created to the reducers.
     */
    public static class PartitionerClass extends Partitioner<Text, Occurrences> {
        public int getPartition(Text key, Occurrences value, int partitionsNumber) {
            return key.hashCode() % partitionsNumber;
        }
    }

    /***
     * * An abstract class for the Splitter's Reduce classes.
     * @field <r1> The occurrence number of the trigram (the key) in the first group of the corpus.
     * @field <r2> The occurrence number of the trigram (the key) in the second group of the corpus.
     * @field <text> The reducer key - the trigram.
     * @param <Occurrences> Indicates the occurrences of the Trigram & its corpus group.
     * @param <KEYOUT>
     * @param <VALUEOUT>
     */
    public abstract static class ReducerSplitter<Text, Occurrences, KEYOUT, VALUEOUT>
            extends Reducer<Text, Occurrences, KEYOUT,
            VALUEOUT> {
        protected long r1;
        protected long r2;
        protected String text;

        @Override
        public void setup(Context context) {
            r1 = 0;
            r2 = 0;
            text = "";
        }

        public abstract void reduce(org.apache.hadoop.io.Text key, Iterable<utils.Occurrences> values,
                                    Context context) throws IOException, InterruptedException;

        protected void reduceLogic(org.apache.hadoop.io.Text key, utils.Occurrences value) {
            if (!key.toString().equals(text)) { // init
                r1 = 0;
                r2 = 0;
                text = key.toString();
            }
            if (value.getCorpus_group()) {
                r1 += value.getCount();
            } else {
                r2 += value.getCount();
            }
        }
    }

    /*** Combine the trigram's Occurrences the mapper created in each server.
     */
    public static class CombinerClass extends ReducerSplitter<Text, Occurrences, Text, Occurrences> {
        public void reduce(Text key, Iterable<Occurrences> values,
                           Context context) throws IOException, InterruptedException {
            for (Occurrences value : values) {  // init
                reduceLogic(key, value);
            }
            context.write(new Text(text), new Occurrences(true, r1));
            context.write(new Text(text), new Occurrences(false, r2));
        }
    }

    /*** Count the occurrences from the trigram's occurrence data arrived from all servers, calculating N and
     * * reduce into one pair of <trigram, r1 r2>
     * @enam <N> The reducer key - the trigram.
     */
    public static class ReducerClass extends ReducerSplitter<Text, Occurrences, Text, Text> {
        public enum Counter {
            N
        }

        public void reduce(Text key, Iterable<Occurrences> values,
                           Context context) throws IOException, InterruptedException {
            for (Occurrences value : values) {
                context.getCounter(Counter.N).increment(value.getCount());
                reduceLogic(key, value);
            }
            context.write(new Text(text), new Text(r1 + " " + r2));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Splitter");
        job.setJarByClass(Splitter.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Occurrences.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); // for running from aws
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(MainLogic.BUCKET_PATH + "/Step1"));// for running from aws
        job.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));  // running from hadoop
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(Splitter.ReducerClass.Counter.N);
        CounterN counterN= CounterN.getInstance();
        counterN.setN(counter.getValue());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
