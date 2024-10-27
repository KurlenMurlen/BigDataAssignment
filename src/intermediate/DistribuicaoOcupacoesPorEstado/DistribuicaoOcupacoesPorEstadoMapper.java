package intermediate.DistribuicaoOcupacoesPorEstado;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistribuicaoOcupacoesPorEstadoMapper extends Mapper<LongWritable, Text, DistribuicaoOcupacoesPorEstadoWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] columns = value.toString().split(",");
        if (columns.length > 12) {
            String estado = columns[1];
            String ocupacao = columns[12];
            context.write(new DistribuicaoOcupacoesPorEstadoWritable(estado, ocupacao), new IntWritable(1));
        }
    }
}
