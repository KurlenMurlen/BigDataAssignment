package intermediate.ContagemRegistrosPorSexoAno;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class ContagemRegistrosPorSexoAno {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args.length < 2) {
            System.err.println("Uso: contagem registros por sexo ano <input path> <output path>");
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

        Job job = Job.getInstance(conf, "contagem de registros por sexo e ano");
        job.setJarByClass(ContagemRegistrosPorSexoAno.class);

        job.setMapperClass(MapperContagem.class);
        job.setCombinerClass(CombinerContagem.class);
        job.setReducerClass(ReduceContagem.class);

        job.setMapOutputKeyClass(AnoSexoWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(AnoSexoWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperContagem extends Mapper<Object, Text, AnoSexoWritable, IntWritable> {
        private boolean isHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] data = value.toString().split(",");

            if (data.length > 6) {
                String ano = data[2].trim();
                String sexo = data[6].trim();

                context.write(new AnoSexoWritable(ano, sexo), new IntWritable(1));
            }
        }
    }

    public static class CombinerContagem extends Reducer<AnoSexoWritable, IntWritable, AnoSexoWritable, IntWritable> {
        public void reduce(AnoSexoWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class ReduceContagem extends Reducer<AnoSexoWritable, IntWritable, AnoSexoWritable, IntWritable> {
        public void reduce(AnoSexoWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class AnoSexoWritable implements WritableComparable<AnoSexoWritable> {
        private Text ano;
        private Text sexo;

        public AnoSexoWritable() {
            this.ano = new Text();
            this.sexo = new Text();
        }

        public AnoSexoWritable(String ano, String sexo) {
            this.ano = new Text(ano);
            this.sexo = new Text(sexo);
        }

        public void write(DataOutput out) throws IOException {
            ano.write(out);
            sexo.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            ano.readFields(in);
            sexo.readFields(in);
        }

        public int compareTo(AnoSexoWritable o) {
            int cmp = ano.compareTo(o.ano);
            if (cmp != 0) {
                return cmp;
            }
            return sexo.compareTo(o.sexo);
        }

        @Override
        public String toString() {
            return ano.toString() + "_" + sexo.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AnoSexoWritable that = (AnoSexoWritable) o;

            if (!ano.equals(that.ano)) return false;
            return sexo.equals(that.sexo);
        }

        @Override
        public int hashCode() {
            int result = ano.hashCode();
            result = 31 * result + sexo.hashCode();
            return result;
        }
    }
}
