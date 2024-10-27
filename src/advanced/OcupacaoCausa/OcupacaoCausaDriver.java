package advanced.OcupacaoCausa;

import advanced.OcupacaoCausa.Job1.OcupacaoCausaMapper;
import advanced.OcupacaoCausa.Job1.OcupacaoCausaReducer;
import advanced.OcupacaoCausa.Job2.OcupacaoCausaMapper2;
import advanced.OcupacaoCausa.Job2.OcupacaoCausaReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class OcupacaoCausaDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // Configura o logger básico
        BasicConfigurator.configure();

        // Executa o driver com os argumentos
        int result = ToolRunner.run(new Configuration(), new OcupacaoCausaDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Verificação de argumentos
        if (args.length != 3) {
            System.err.println("Uso: OcupacaoCausaDriver <caminho entrada> <caminho intermediário> <caminho saída final>");
            return -1; // Retorna -1 se os argumentos estiverem errados
        }

        // Configuração do Hadoop
        Configuration conf = new Configuration();

        // Caminhos fornecidos como argumentos
        Path input = new Path(args[0]); // Caminho do arquivo de entrada
        Path intermediate = new Path(args[1]); // Saída intermediária
        Path output = new Path(args[2]); // Saída final

        // Manipulação do sistema de arquivos
        FileSystem fs = FileSystem.get(conf);

        // Limpa diretórios de saída existentes
        if (fs.exists(intermediate)) fs.delete(intermediate, true);
        if (fs.exists(output)) fs.delete(output, true);

        // --- Job 1: Agregação de Ocupação e Causa de Morte ---
        Job job1 = Job.getInstance(conf, "Job 1: Agregação de Ocupação e Causa de Morte");

        // Configura o Job 1
        job1.setJarByClass(OcupacaoCausaDriver.class);
        job1.setMapperClass(OcupacaoCausaMapper.class); // Mapper do Job 1
        job1.setReducerClass(OcupacaoCausaReducer.class); // Reducer do Job 1

        // Define os tipos de chave e valor de saída do Mapper
        job1.setMapOutputKeyClass(EstadoOcupCausaWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // Define os tipos de chave e valor de saída do Job 1
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Define os caminhos de entrada e saída
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediate);

        // Executa o Job 1 e verifica sucesso
        boolean successJob1 = job1.waitForCompletion(true);

        if (successJob1) {
            // --- Job 2: Cálculo de Percentual por Ocupação e Causa de Morte ---
            Job job2 = Job.getInstance(conf, "Job 2: Cálculo de Percentual por Ocupação e Causa de Morte");

            // Configura o Job 2
            job2.setJarByClass(OcupacaoCausaDriver.class);
            job2.setMapperClass(OcupacaoCausaMapper2.class); // Mapper do Job 2
            job2.setCombinerClass(OcupacaoCausaReducer2.class);
            job2.setReducerClass(OcupacaoCausaReducer2.class); // Reducer do Job 2

            // Define os tipos de chave e valor de saída do Mapper do Job 2
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            // Define os tipos de chave e valor de saída do Job 2
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // Define os caminhos de entrada e saída
            FileInputFormat.addInputPath(job2, intermediate);
            FileOutputFormat.setOutputPath(job2, output);

            // Executa o Job 2 e retorna sucesso ou falha
            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return 1; // Retorna 1 se o Job 1 falhar
    }
}
