package advanced.OcupacaoCausa.Job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OcupacaoCausaMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length == 4) {
            // Exemplo: chave é (estado + ocupação), valor é causa básica + total
            String estadoOcup = fields[0] + "\t" + fields[1]; // Concatenar estado e ocupação
            String causaETotal = fields[2] + "\t" + fields[3]; // Concatenar causa básica e o total

            context.write(new Text(estadoOcup), new Text(causaETotal));
        }
    }
}
