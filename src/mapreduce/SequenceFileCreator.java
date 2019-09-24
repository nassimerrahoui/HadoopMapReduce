package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class SequenceFileCreator {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
			
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//String[] otherArgs=args;
		 if (otherArgs.length != 2) {
		      System.err.println("Usage: SequenceFileCreator <path_hdfs_dest> <nb_entries>");
		      System.exit(2);
		  }
		String path_dest = otherArgs[0];
		int numEntries = Integer.parseInt(otherArgs[1]); 
		final Path file = new Path(path_dest);
		final SequenceFile.Writer writer = SequenceFile.createWriter( conf, Writer.file(file), Writer.keyClass(LongWritable.class), Writer.valueClass(Text.class),Writer.compression(CompressionType.NONE));

		try{	
			
		
			for (int i = 0; i < numEntries; i++) {
			
				final LongWritable key = new LongWritable((i%4));
				final Text value = new Text("Valeur "+i);//valeur du map i
				writer.append(key, value);
			
			
			}
		} finally{
			writer.close();
		}
	}

}
