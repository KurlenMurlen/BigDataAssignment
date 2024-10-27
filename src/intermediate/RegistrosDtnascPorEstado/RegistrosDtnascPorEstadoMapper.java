package intermediate.RegistrosDtnascPorEstado;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RegistrosDtnascPorEstadoMapper extends Mapper<LongWritable, Text, EstadoAnoNascimentoWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] columns = value.toString().split(",");
        if (columns.length > 5) {
            String estado = columns[1];
            String anoNasc = columns[5];
            context.write(new EstadoAnoNascimentoWritable(estado, anoNasc), new IntWritable(1));
        }
    }
}
