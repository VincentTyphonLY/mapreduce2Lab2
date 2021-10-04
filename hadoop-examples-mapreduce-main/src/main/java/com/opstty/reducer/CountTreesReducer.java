package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountTreesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable compt = new IntWritable();

    public void reduce(Text genreKey, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        compt.set(sum);
        context.write(genreKey, compt);
    }
}
