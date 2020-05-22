import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// CLASE PRINCIPAL
public class WordCount {

    public static class CustomMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text wordMap = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String clean = value.toString().replaceAll("[^\\w\\s]", ""); // Limpio caracteres especiales
            String[] stringArray = clean.split("\\s+"); // convierto en array
            for (String str : stringArray) {
                wordMap.set(str); // agrego a mi clase text de hadoop
                context.write(wordMap, new IntWritable(1));
            }
        }
    }

    public static class CustomReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0; // declaro el contador
            for (IntWritable val : values) {
                sum += val.get(); // aumento el valor devuelto por otros
            }
            result.set(sum); // actualizo el valor
            context.write(key, result); // devuelvo el resultado
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(); // Instancio la clase job
        job.setJarByClass(WordCount.class); // Indico la clase principal que se convertira en .jar
        job.setMapperClass(CustomMap.class); // Selecciono mi mapper
        job.setReducerClass(CustomReduce.class); // mi reducer
        job.setOutputKeyClass(Text.class); // El formato del output
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // input
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}