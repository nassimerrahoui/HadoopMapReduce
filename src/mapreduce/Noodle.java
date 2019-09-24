package mapreduce;

import java.io.IOException;
import java.util.HashMap;

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


public class Noodle {

	public static class NoodleMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text tranche = new Text();
		private Text value = new Text();

		public String getHeures(String s) {
			String[] s_split = s.split("\\s");
			System.err.println("split"+s_split.length);
			String[] split_heure = s_split[0].split("_");
			System.err.println("heures"+split_heure.length);
			return split_heure[3];
		}

		public String getMinutes(String s) {
			String[] s_split = s.split("\\s");
			System.err.println("split"+s_split.length);
			String[] split_minutes = s_split[0].split("_");
			System.err.println("minutes"+split_minutes.length);
			return split_minutes[4];
		}

		public Text getTranche(String heures, String minutes) {
			String s = "entre " + heures + "h";
			if (Integer.valueOf(minutes) < 30) {
				s += "00 et " + heures + "h29";
			} else {
				s += "30 et " + heures + "h59";
			}
			Text t  = new Text();
			t.set(s);
			return t;
		}

		public Text getRequete(String s) {
			String[] s_split = s.split("\\s");
			Text t  = new Text();
			t.set(s_split[2]);
			return t;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			tranche.set(getTranche(getHeures(value.toString()), getMinutes(value.toString())));
			value.set(getRequete(value.toString()));
			context.write(tranche, value);
		}
	}

	public static class NoodleReducer extends Reducer<Text, Text, Text, IntWritable> {

		private Text mot = new Text();
		private IntWritable value = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> hm = new HashMap<>();
			String max = "";
			int cpt = 0;
			for (Text s : values) {
				String[] s_split = s.toString().split("\\+"); 
				for(String sb : s_split)
				if (!hm.containsKey(sb)) {
					hm.put(sb, 1);
					if (1 >= cpt) {
						cpt = 1;
						max = sb;
					}
				} else {
					hm.replace(sb, hm.get(sb) + 1);
					if (hm.get(sb) >= cpt) {
						cpt = hm.get(sb);
						max = sb;
					}
				}
			}
			mot.set(key+" "+max);
			value.set(cpt);
			context.write(mot, value);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) { System.err.println("Usage: noodle <in> <out>");
		System.exit(2); }
		
		Job job = Job.getInstance(conf, "noodle");
		job.setJarByClass(Noodle.class);// permet d'indiquer le jar qui contient l'ensemble des .class du job à partir
										// d'un nom de classe
		job.setMapperClass(NoodleMapper.class); // indique la classe du Mapper
		job.setReducerClass(NoodleReducer.class); // indique la classe du Reducer
		job.setMapOutputKeyClass(Text.class);// indique la classe de la clé sortie map
		job.setMapOutputValueClass(Text.class);// indique la classe de la valeur sortie map
		job.setOutputKeyClass(Text.class);// indique la classe de la clé de sortie reduce
		job.setOutputValueClass(IntWritable.class);// indique la classe de la clé de sortie reduce
		job.setInputFormatClass(TextInputFormat.class); // indique la classe du format des données d'entrée
		job.setOutputFormatClass(TextOutputFormat.class); // indique la classe du format des données de sortie
		// job.setPartitionerClass(HashPartitioner.class);// indique la classe du
		// partitionneur
		job.setNumReduceTasks(1);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur
									// (1 par défaut)
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));// indique le ou les chemins HDFS d'entrée
		final Path outDir = new Path(otherArgs[1]);// indique le chemin du dossier de sortie
		FileOutputFormat.setOutputPath(job, outDir);
		final FileSystem fs = FileSystem.get(conf);// récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	}
}