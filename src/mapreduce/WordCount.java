package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class WordCountMapper 
       extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class WordCountReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  
  
 
  
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("mapreduce.map.speculative", true);
	conf.setBoolean("mapreduce.reduce.speculative", true);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
    job.setMapperClass(WordCountMapper.class); // indique la classe du Mapper
    job.setReducerClass(WordCountReducer.class); // indique la classe du Reducer
    job.setMapOutputKeyClass(Text.class);// indique la classe  de la clé sortie map
    job.setMapOutputValueClass(IntWritable.class);// indique la classe  de la valeur sortie map    
    job.setOutputKeyClass(Text.class);// indique la classe  de la clé de sortie reduce    
    job.setOutputValueClass(IntWritable.class);// indique la classe  de la clé de sortie reduce
    job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
    job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
    //job.setPartitionerClass(HashPartitioner.class);// indique la classe du partitionneur
    job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
    
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//indique le ou les chemins HDFS d'entrée
    final Path outDir = new Path(otherArgs[1]);//indique le chemin du dossier de sortie
    FileOutputFormat.setOutputPath(job, outDir);
    final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
	 if (fs.exists(outDir)) { // test si le dossier de sortie existe
		 fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
	 }
   
    System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
  }
}
