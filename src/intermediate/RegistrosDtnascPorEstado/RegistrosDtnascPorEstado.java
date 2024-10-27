package intermediate.RegistrosDtnascPorEstado;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class RegistrosDtnascPorEstado extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RegistrosDtnascPorEstado <input path> <output path>");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Registros Dtnasc Por Estado");

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job, input);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(RegistrosDtnascPorEstado.class);
        job.setMapperClass(RegistrosDtnascPorEstadoMapper.class);
        job.setCombinerClass(RegistrosDtnascPorEstadoCombiner.class);
        job.setReducerClass(RegistrosDtnascPorEstadoReducer.class);

        job.setMapOutputKeyClass(EstadoAnoNascimentoWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(EstadoAnoNascimentoWritable.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new RegistrosDtnascPorEstado(), args);
        System.exit(result);
    }
}
