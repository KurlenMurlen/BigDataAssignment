package basic.DistribuicaoOcupacoes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistribuicaoOcupacoesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] columns = line.split(",");
        if (columns.length > 12) {
            String Ocupacao = columns[12];
            context.write(new Text(Ocupacao), new IntWritable(1));
        }
    }
}
