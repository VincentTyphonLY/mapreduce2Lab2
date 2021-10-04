package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, Float> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // We filter line base on header value to skip header
        if (!value.toString().contains("ESPECE")) {
            Text species = new Text(value.toString().split(";")[3]);
            Float hauteur = new Float(value.toString().split(";")[6]);
            context.write(species, hauteur);
        }
    }
}
