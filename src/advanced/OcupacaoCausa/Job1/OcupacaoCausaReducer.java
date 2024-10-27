package advanced.OcupacaoCausa.Job1;

import advanced.OcupacaoCausa.EstadoOcupCausaWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OcupacaoCausaReducer extends Reducer<EstadoOcupCausaWritable, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(EstadoOcupCausaWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Soma todos os valores associados a esta chave (estado, ocupação, causa básica)
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Saída: chave (estado, ocupação, causa básica) e o total como valor
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}
