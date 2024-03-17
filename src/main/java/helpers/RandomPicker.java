package helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * The RandomPicker class is a helper class for selecting a random sample of lines from a file.
 * It uses a MapReduce job to assign a random key to each line and then sorts the lines by the random key.
 * */
public class RandomPicker {
    public static class RandomPickerMapper extends Mapper<Object, Text, IntWritable, Text>{
        private static final IntWritable randomKey = new IntWritable();
        private static final Random rand = new Random();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Generating random key for sorting randomly in reducer
            randomKey.set(rand.nextInt());
            context.write(randomKey, value);
        }
    }

    public static class RandomLineReducer extends Reducer<IntWritable, Text, Text, Text> {

        private int linesSelected = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int NUM_LINES_TO_SELECT = Integer.parseInt(context.getConfiguration().get("n"));
            for (Text value : values) {
                if (linesSelected < NUM_LINES_TO_SELECT) {
                    context.write(new Text(), value);
                    linesSelected++;
                } else {
                    break; // Stop processing after selecting required number of lines
                }
            }
        }
    }

    /**
     * Selects a random sample of n lines from the input directory.
     * @param inputDir The input directory
     * @param n The number of lines to select
     * @return An array of n lines selected at random
     * */
    public String[] pickRandom(String inputDir, int n) throws IOException, ClassNotFoundException, InterruptedException {
        String outputDir = "random-picker-output" + System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("n", String.valueOf(n));
        Job job = Job.getInstance(conf, "Random Picker");
        job.setJarByClass(RandomPicker.class);
        job.setMapperClass(RandomPickerMapper.class);
        job.setReducerClass(RandomLineReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.waitForCompletion(true);
        String[] pickedLines = HDFSReader.read(conf, outputDir);
        for(int i = 0; i < pickedLines.length; i++) {
            pickedLines[i] = pickedLines[i].split("\t")[1];
        }
        FileSystem.get(conf).delete(new Path(outputDir), true);
        return pickedLines;
    }

    // Test the RandomPicker class
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2) {
            System.err.println("Usage: helpers.RandomPicker <input-dir> <n>");
            System.exit(-1);
        }

        RandomPicker randomPicker = new RandomPicker();
        String[] pickedLines = randomPicker.pickRandom(args[0], Integer.parseInt(args[1]));
        for (String line : pickedLines) {
            System.out.println(line);
        }
    }
}
