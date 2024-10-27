package advanced.AssistenciaCorrelation.Job1;

import advanced.AssistenciaCorrelation.AssistenciaMedicaValue;
import advanced.AssistenciaCorrelation.MunicipioAnoEstCivKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AssistenciaMapper extends Mapper<LongWritable, Text, MunicipioAnoEstCivKey, AssistenciaMedicaValue> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignora o cabeçalho
        if (key.get() == 0 && value.toString().contains("estado")) {
            return;
        }

        // Divide o CSV em colunas
        String[] fields = value.toString().split(",");
        if (fields.length > 10) {
            String municipio = fields[13].trim();  // CODMUNRES
            int ano = Integer.parseInt(fields[2].trim());  // Ano
            String estCivil = fields[10].trim();  // ESTCIV
            String assistMed = fields[8].trim();  // ASSISTMED

            int assistencia = (assistMed.equalsIgnoreCase("Sim") ? 1 : 0);

            // Emite a chave composta e o valor
            context.write(new MunicipioAnoEstCivKey(municipio, ano, estCivil), new AssistenciaMedicaValue(assistencia, 1));
        }
    }
}
