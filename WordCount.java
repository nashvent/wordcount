import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// CLASE PRINCIPAL
public class WordCount {

    public static class CustomMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, javax.xml.soap.Text value, Context context)
                throws IOException, InterruptedException {
            // Limpio caracteres especiales y creo un array
            String[] stringArray = value.toString().replaceAll("[-+.^:,]", "").split("\\s+");
            for (String str : stringArray) {
                word.set(str);
                context.write(word, new IntWritable(1));
            }
        }
    }

    // Reduce function
    public static class CustomReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(CustomMap.class);
        job.setReducerClass(CustomReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}