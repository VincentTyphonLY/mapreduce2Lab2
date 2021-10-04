package com.opstty.job;

import com.opstty.mapper.CountTreesMapper;
import com.opstty.mapper.DistrictMapper;
import com.opstty.mapper.SpeciesMapper;
import com.opstty.mapper.TokenizerMapper;
import com.opstty.reducer.CountTreesReducer;
import com.opstty.reducer.DistrictReducer;
import com.opstty.reducer.IntSumReducer;
import com.opstty.reducer.SpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CountTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Count trees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "CountTrees");
        job.setJarByClass(CountTrees.class);
        job.setMapperClass(CountTreesMapper.class);
        job.setCombinerClass(CountTreesReducer.class);
        job.setReducerClass(CountTreesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
