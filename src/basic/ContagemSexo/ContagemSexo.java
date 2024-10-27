package basic.ContagemSexo;

import java.io.IOException;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class ContagemSexo {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args.length < 2) {
            System.err.println("Uso: contagem sexo <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "contagem de sexo");
        job.setJarByClass(ContagemSexo.class);

        job.setMapperClass(MapperContagem.class);
        job.setReducerClass(ReduceContagem.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperContagem extends Mapper<Object, Text, Text, IntWritable> {

        private boolean isHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] genderData = value.toString().split(",");


            if (genderData.length > 6) {
                String gender = genderData[6].trim();
                context.write(new Text(gender), new IntWritable(1));
            }
        }
    }

    public static class ReduceContagem extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(key, new IntWritable(count));
        }
    }
}

