package advanced.AssistenciaCorrelation;

import advanced.AssistenciaCorrelation.Job1.AssistenciaMapper;
import advanced.AssistenciaCorrelation.Job1.AssistenciaReducer;
import advanced.AssistenciaCorrelation.Job2.CorrelationMapper;
import advanced.AssistenciaCorrelation.Job2.CorrelationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class AssistenciaCorrelationDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // Configura o logger básico para Hadoop
        BasicConfigurator.configure();

        // Executa o driver com os argumentos de entrada
        int result = ToolRunner.run(new Configuration(), new AssistenciaCorrelationDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Verificação dos argumentos
        if (args.length != 3) {
            System.err.println("Uso: AssistenciaCorrelationDriver <caminho entrada> <caminho intermediário> <caminho saída final>");
            return -1;
        }

        // Configuração e caminho dos arquivos
        Configuration conf = new Configuration();
        Path input = new Path(args[0]); // Caminho do arquivo de entrada
        Path intermediate = new Path(args[1]); // Caminho para a saída intermediária
        Path output = new Path(args[2]); // Caminho para a saída final

        // Manipulação do sistema de arquivos
        FileSystem fs = FileSystem.get(conf);
        // Limpa diretórios de saída existentes
        if (fs.exists(intermediate)) fs.delete(intermediate, true);
        if (fs.exists(output)) fs.delete(output, true);

        // --- Job 1: Processamento de Assistência Médica ---
        Job job1 = Job.getInstance(conf, "Job 1: Agregação de Assistência Médica");

        // Configurações do Job 1
        job1.setJarByClass(AssistenciaCorrelationDriver.class);
        job1.setMapperClass(AssistenciaMapper.class);
        job1.setCombinerClass(AssistenciaReducer.class);  // Adiciona o Combiner
        job1.setReducerClass(AssistenciaReducer.class);

        job1.setMapOutputKeyClass(MunicipioAnoEstCivKey.class); // Chave do mapper
        job1.setMapOutputValueClass(AssistenciaMedicaValue.class); // Valor do mapper
        job1.setOutputKeyClass(MunicipioAnoEstCivKey.class); // Chave do reducer
        job1.setOutputValueClass(AssistenciaMedicaValue.class); // Valor do reducer

        // Caminhos de entrada e saída
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediate);

        // Executa o Job 1 e verifica sucesso
        if (job1.waitForCompletion(true)) {
            // --- Job 2: Cálculo de Percentual ---
            Job job2 = Job.getInstance(conf, "Job 2: Cálculo de Percentual");

            // Configurações do Job 2
            job2.setJarByClass(AssistenciaCorrelationDriver.class);
            job2.setMapperClass(CorrelationMapper.class);
            job2.setReducerClass(CorrelationReducer.class);
            job2.setMapOutputKeyClass(Text.class); // Chave do mapper
            job2.setMapOutputValueClass(AssistenciaMedicaValue.class); // Valor do mapper
            job2.setOutputKeyClass(Text.class); // Chave do reducer
            job2.setOutputValueClass(AssistenciaMedicaValue.class); // Valor do reducer

            // Caminhos de entrada e saída
            FileInputFormat.addInputPath(job2, intermediate);
            FileOutputFormat.setOutputPath(job2, output);

            // Executa o Job 2 e retorna sucesso ou falha
            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return 1; // Retorna 1 se o Job 1 falhar
    }
}
