package advanced.customwritable;

import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // registros das classes;
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombinerForAverage.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // O map executa linha a linha :: Converte a linha em string
            String linha = value.toString();

            // fazendo o split para quebrara a linha passando o caracter ',' como valor de quebra
            String [] coluna = linha.split(",");

            // obtendo a temperatuda :: converte o valor da outava coluna e float
            double temp = Double.parseDouble(coluna[8]);

            //
            String mes = coluna[2];

            // registo de ocorrencia
            long n = 1;

            // passando a temperatura para o sort/shuffle

                // chamada do context passando como chave os meses;
            con.write(new Text(mes),new FireAvgTempWritable(n,temp));
                // chamada do context passando uma unica chave para calculo da media geral;
            con.write(new Text("media de tudo"),new FireAvgTempWritable(n,temp));
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            /*
            Chega no reduce uma chave unica (media de tudo) com uma lista de valores compostos por
            ocorrencia e soma das temperaturas
            */
            double somaTemps = 0.0;
            long somaNs = 0;
            // Somando temperaturas e ocorrencias
            for(FireAvgTempWritable o : values){
                somaTemps += o.getSomaTemp();
                somaNs += o.getOcorrencia();
            }
            // chamada do context passando uma unica chave "media";
            //con.write(new Text("media"), new DoubleWritable(somaTemps / somaNs));

            // chamada do context com as chaves (key) vindas da saída do map;
            con.write(new Text(key), new DoubleWritable(somaTemps / somaNs));
        }
    }

    // o combiner é uma etapa opcional
    public static class CombinerForAverage extends Reducer< Text , FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            // no combiner, vamos somar as temperaturas parciais do bloco e tambem  as ocorrencias
            double somaTemps = 0.0;
            long somaNs = 0;
            // Somando temperaturas e ocorrencias
            for(FireAvgTempWritable o : values){
                somaTemps += o.getSomaTemp();
                somaNs += o.getOcorrencia();
            }
            // passando para o reduce alguns resultados ja somados,
            // isto é, para cada chave;
            // ja temos as somas das temperaturas e ocorrencias;
            con.write(key, new FireAvgTempWritable(somaNs,somaTemps));
        }
    }
}
