import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogSuccessfulRequestsDriver {
    public static void main(String[] args) throws Exception {
        // Configuration de l'environnement Hadoop
        Configuration conf = new Configuration();

        // Création d'un nouveau job avec une description
        Job job = Job.getInstance(conf, "Count Successful Requests");

        // Spécification de la classe principale qui contient le jar
        job.setJarByClass(LogSuccessfulRequestsDriver.class);

        // Spécification du Mapper et du Reducer pour ce job
        job.setMapperClass(LogMapper2.class);
        job.setReducerClass(LogReducer2.class);

        // Spécification des types de sortie du Mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Ajout des chemins des fichiers d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécution du job et terminaison du programme avec un code de sortie (0 pour succès, 1 pour échec)
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
