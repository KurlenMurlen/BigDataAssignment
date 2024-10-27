package intermediate.DistribuicaoOcupacoesPorEstado;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistribuicaoOcupacoesPorEstadoCombiner extends Reducer<DistribuicaoOcupacoesPorEstadoWritable, IntWritable, DistribuicaoOcupacoesPorEstadoWritable, IntWritable> {
    public void reduce(DistribuicaoOcupacoesPorEstadoWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
