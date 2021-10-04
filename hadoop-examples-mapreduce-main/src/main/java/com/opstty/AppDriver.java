package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("counttrees", CountTrees.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("maxheight", MaxHeight.class,
                    "A map/reduce program that counts the words in the input files.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
