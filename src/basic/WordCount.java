package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {
    public static void main(String[] args) throws Exception {
        // faz uma configuração simples do Hadoop
        BasicConfigurator.configure();

        // definição dos arquivos;
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // tendo os arquivos de entrada e saida vamos pro job  !!MUITA ATENÇAO!!
        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // Registro das Classes
        j.setJarByClass(WordCount.class); //main
        j.setMapperClass(MapForWordCount.class); //map
        j.setReducerClass(ReduceForWordCount.class); // reduce

        // Definição dos tipos de saída (map e reduce);
        j.setMapOutputKeyClass(Text.class); // chave de saida do Map
        j.setMapOutputValueClass(IntWritable.class); // valor de saida do Map
        j.setOutputKeyClass(Text.class); // chave de saida do Reduce
        j.setOutputValueClass(IntWritable.class); // valor de saida do Reduce

        // Cadastrar arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); // entrada
        FileOutputFormat.setOutputPath(j, output); // saida

        // Rodar o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
        /*system.exit é o relatorio final se deu erro (1) ou nao (0).
        wait for completion aguarda a execução do job;
        */
    }

    /**     MAP
     * parametro 1  :: Tipo da chave de entrada, no exemplo (LongWritable);
     * parametro 2  :: Tipo de valor da entrada, no exemplo (Text);
     * parametro 3  :: Tipo da chave de saída, no exemplo (Text);
     * parametro 4  :: Tipo do valor de saída, no exemplo (IntWritable);
     *
     * // QUANDO A ENTRADA É UM TEXTO //
     *  - input: (offset, conteúdo da linha); [offset == QUANTIDADE DE BITES DESDE O COMEÇO DO ARQUIVO]
     *  OBS: longWritable, IntWritable, Text por "baixo dos panos" é o mesmo que Long , Int e String,
     *  o que difere é o Writable, que nos diz que o tipo é serializavel (permite converter objeto em bytes),
     *  com o objetivo de facilitar a etapa de transferência.
     */
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // obtendo o conteúdo da linha
            String linha= value.toString();
            // quebrando a linha em palavras
            String[] palavras = linha.split(" ");
            // loop para criar as túplas (chave,valor) :: no exemplo (palavra,1)
            for(String p : palavras){
                // variável interna do loop que pega as chaves
                Text chaveSaida = new Text(p);
                // variável interna do loop que atribui o valor da túpla
                IntWritable valorSaida = new IntWritable(1);
                /* o contexto é o responsavel em fazer a comunicação do map com o shuffle e reduce
                    ele também é responsável pela parte do Sort/Shuffle por baixo dos panos
                 */
                con.write(chaveSaida,valorSaida);
                /*
                 !! Atentar se os tipos  de saida do 'MapForWordCount' batem com o 'void map' e com as
                 variáveis internas do loop de montagem das túplas !!
                */
            }
        }
    }
    /**     REDUCE
     * parãmetro 1  :: Tipo da chave de entrada, no exemplo (Text) (igual a chave  de saída do map);
     * parâmetro 2  :: Tipo de valor da entrada, no exemplo (IntWritable) (igual ao valor  de saída do map);
     * parâmetro 3  :: Tipo da chave de saída, no exemplo (Text);
     * parâmetro 4  :: Tipo do valor de saída, no exemplo (IntWritable);
     *
     */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma=0;
            // somando todos valores de ocorrência para cada palavra
            for(IntWritable o : values){
                soma += o.get();
            }
            // criação das túplas de saída (chave,valor)
            // reaproveita a chave e organiza o valor.
            IntWritable valorSaida = new IntWritable(soma);
            // salvando em arquivo :: usamos o contexto
            con.write(key,valorSaida);
        }
    }

}
