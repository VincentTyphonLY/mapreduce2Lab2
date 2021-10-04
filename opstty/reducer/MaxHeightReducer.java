package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable max = new IntWritable();

    public void reduce(Text species, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int comp = 0;
        for (IntWritable val : values) {
            if (comp < val.get()){
                comp = val.get();
            }
        }
        max.set(comp);
        context.write(species, max);
    }
}
