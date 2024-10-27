package intermediate.ContagemSexoEstado;

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

public class ContagemSexoEstado {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args.length < 2) {
            System.err.println("Uso: contagem sexo estado <input path> <output path>");
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

        Job job = Job.getInstance(conf, "contagem de sexo e estado");
        job.setJarByClass(ContagemSexoEstado.class);

        job.setMapperClass(MapperContagem.class);
        job.setCombinerClass(CombinerContagem.class);
        job.setReducerClass(ReduceContagem.class);

        job.setMapOutputKeyClass(EstadoSexoWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(EstadoSexoWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapperContagem extends Mapper<Object, Text, EstadoSexoWritable, IntWritable> {

        private boolean isHeader = true;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] data = value.toString().split(",");

            if (data.length > 6) {
                String estado = data[1].trim();
                String sexo = data[6].trim();

                EstadoSexoWritable compositeKey;

                if (sexo.isEmpty()) {
                    compositeKey = new EstadoSexoWritable(estado, "Desconhecido");
                } else {
                    compositeKey = new EstadoSexoWritable(estado, sexo);
                }

                context.write(compositeKey, new IntWritable(1));
            }
        }
    }

    public static class CombinerContagem extends Reducer<EstadoSexoWritable, IntWritable, EstadoSexoWritable, IntWritable> {
        public void reduce(EstadoSexoWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class ReduceContagem extends Reducer<EstadoSexoWritable, IntWritable, EstadoSexoWritable, IntWritable> {
        public void reduce(EstadoSexoWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class EstadoSexoWritable implements WritableComparable<EstadoSexoWritable> {
        private Text estado;
        private Text sexo;

        public EstadoSexoWritable() {
            this.estado = new Text();
            this.sexo = new Text();
        }

        public EstadoSexoWritable(String estado, String sexo) {
            this.estado = new Text(estado);
            this.sexo = new Text(sexo);
        }

        public void write(DataOutput out) throws IOException {
            estado.write(out);
            sexo.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            estado.readFields(in);
            sexo.readFields(in);
        }

        public int compareTo(EstadoSexoWritable o) {
            int cmp = estado.compareTo(o.estado);
            if (cmp != 0) {
                return cmp;
            }
            return sexo.compareTo(o.sexo);
        }

        @Override
        public String toString() {
            return estado.toString() + "_" + sexo.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EstadoSexoWritable that = (EstadoSexoWritable) o;

            if (!estado.equals(that.estado)) return false;
            return sexo.equals(that.sexo);
        }

        @Override
        public int hashCode() {
            int result = estado.hashCode();
            result = 31 * result + sexo.hashCode();
            return result;
        }
    }
}


