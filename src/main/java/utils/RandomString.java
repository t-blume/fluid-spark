package utils;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

public class RandomString {


    public static void main(String[] args) throws IOException {
        /**
         * some test stuff
         */
        int intervals = 50;
        int minEdges = 5000;
        int maxEdges = 10000;
        //percentage
        double overlap = 0.2;

        Random randomNumber = new Random();

        //label props (Alphabet = 36 chars)

        int possibleEdgeLabel = 100;
        int possibleIDValues = 50;
        int possibleSourceValues = 20;

        int minEdgesPerVertex = 3;
        int maxEdgesPerVertex = 10;


        int numberOfEdgeLabel = 1;
        while (Math.pow(lower.length(), numberOfEdgeLabel) < possibleEdgeLabel)
            numberOfEdgeLabel++;

        System.out.println(numberOfEdgeLabel);
        RandomString randomLabel = new RandomString(numberOfEdgeLabel, new SecureRandom(), lower);

        String typeLabel = randomLabel.nextString();
        System.out.println("Type Label: " + typeLabel);


        int numberOfIDs = 1;
        while (Math.pow(lower.length(), numberOfIDs) < possibleIDValues)
            numberOfIDs++;
        System.out.println(numberOfIDs);
        RandomString randomID = new RandomString(numberOfIDs, new SecureRandom(), digits);

        int numberOfSources = 1;
        while (Math.pow(lower.length(), numberOfSources) < possibleSourceValues)
            numberOfSources++;
        System.out.println(numberOfSources);
        RandomString randomSource = new RandomString(numberOfSources, new SecureRandom(), lower);


        String baseFilename = "interval-";
        String extenstion = ".nq";
        String foldername = "resources/random-graph-own";
        File folder = new File(foldername);
        if (!folder.exists())
            folder.mkdirs();

        List<Tuple2<String, String>> overlappingEdges = new LinkedList<>();
        for (int i = 0; i < intervals; i++) {
            System.out.println("Interval: " + i);
            List<String> forbiddenIds = new LinkedList<>();
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(foldername + "/" + baseFilename + i + extenstion)));
            for (Tuple2<String, String> overlapEdge : overlappingEdges) {
                forbiddenIds.add(overlapEdge._1);
                writer.write(overlapEdge._2);
            }
            overlappingEdges.clear();



            int edges = randomNumber.nextInt(maxEdges - minEdges) + minEdges;

            int edgesWritten = 0;
            while (edgesWritten < edges && forbiddenIds.size() < possibleIDValues){

                //write new edges
                String newEdge = "";
                String source = randomID.nextString();
                while (forbiddenIds.contains(source))
                    source = randomID.nextString();

                boolean keepThisInstance = false;
                //some percentage of instances are forces to not change at all
                double diceRoll = randomNumber.nextDouble();
                if(diceRoll <= overlap)
                    keepThisInstance = true;

                int localEdges = randomNumber.nextInt(maxEdgesPerVertex - minEdgesPerVertex) + minEdgesPerVertex;
                for(int e=0; e < localEdges; e++){
                    String target = randomID.nextString();

                    while (source == target)
                        target = randomID.nextString();

                    newEdge += "<" + source + "> <" + randomLabel.nextString() + "> <" + target + "> <" + randomSource.nextString() + "> .\n";

                    if (keepThisInstance)
                        overlappingEdges.add(new Tuple2<>(source, newEdge));

                    writer.write(newEdge);
                    edgesWritten++;
                }
                forbiddenIds.add(source);
            }
            forbiddenIds.clear();
            writer.close();
        }


    }

    /**
     * Generate a random string.
     */
    public String nextString() {
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }

    public static final String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static final String lower = upper.toLowerCase(Locale.ROOT);

    public static final String digits = "0123456789";

    public static final String alphanum = upper + lower + digits;

    private final Random random;

    private final char[] symbols;

    private final char[] buf;

    public RandomString(int length, Random random, String symbols) {
        if (length < 1) throw new IllegalArgumentException();
        if (symbols.length() < 2) throw new IllegalArgumentException();
        this.random = Objects.requireNonNull(random);
        this.symbols = symbols.toCharArray();
        this.buf = new char[length];
    }

    /**
     * Create an alphanumeric string generator.
     */
    public RandomString(int length, Random random) {
        this(length, random, alphanum);
    }

    /**
     * Create an alphanumeric strings from a secure generator.
     */
    public RandomString(int length) {
        this(length, new SecureRandom());
    }

    /**
     * Create session identifiers.
     */
    public RandomString() {
        this(21);
    }

}