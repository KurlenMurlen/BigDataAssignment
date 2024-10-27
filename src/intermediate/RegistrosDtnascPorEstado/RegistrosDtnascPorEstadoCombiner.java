package intermediate.RegistrosDtnascPorEstado;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RegistrosDtnascPorEstadoCombiner extends Reducer<EstadoAnoNascimentoWritable, IntWritable, EstadoAnoNascimentoWritable, IntWritable> {
    public void reduce(EstadoAnoNascimentoWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
