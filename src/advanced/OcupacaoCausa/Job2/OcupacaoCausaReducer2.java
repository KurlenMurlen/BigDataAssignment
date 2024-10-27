package advanced.OcupacaoCausa.Job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OcupacaoCausaReducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        StringBuilder causas = new StringBuilder();

        // Exemplo: soma os totais e concatena as causas básicas
        for (Text value : values) {
            String[] causaETotal = value.toString().split("\t");
            causas.append(causaETotal[0]).append(" "); // Concatenar causa básica
            total += Integer.parseInt(causaETotal[1]); // Somar o total
        }

        // Saída: chave é (estado + ocupação), valor é causas concatenadas e o total
        context.write(key, new Text(causas.toString() + "\t" + total));
    }
}
