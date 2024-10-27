package advanced.OcupacaoCausa.Job1;

import advanced.OcupacaoCausa.EstadoOcupCausaWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OcupacaoCausaMapper extends Mapper<LongWritable, Text, EstadoOcupCausaWritable, IntWritable> {

    private final EstadoOcupCausaWritable estadoOcupCausaKey = new EstadoOcupCausaWritable();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Divide a linha do CSV
        String[] fields = value.toString().split(",");

        if (fields.length >= 14) { // Verifica se a linha tem pelo menos 14 colunas
            String estado = fields[1].trim(); // Campo estado
            String ocupacao = fields[12].trim(); // Campo ocupação
            String causaBasica = fields[14].trim(); // Campo causa básica (CID-10)

            // Define a chave composta (estado, ocupação, causa básica)
            estadoOcupCausaKey.setEstado(estado);
            estadoOcupCausaKey.setOcupacao(ocupacao);
            estadoOcupCausaKey.setCausaBasica(causaBasica);

            // Emite a chave composta e o valor 1
            context.write(estadoOcupCausaKey, one);
        }
    }
}
