//package apriori;

import edu.rit.pj2.vbl.LongVbl;
import edu.rit.pjmr.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.Date;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.concurrent.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;


/**
 * Class AprioriMR is the main program for the PJMR map-reduce job that analyses log files for
 * frequent itemset mining
 */

public class AprioriMR
        extends PjmrJob<TextId, String,String, LongVbl> {

    static CopyOnWriteArrayList<int[]> itemsets;
    static CopyOnWriteArrayList<int[]> frequentCandidates;
    static int FrequentSets_count = 0;
    static int itemsetNumber = 1;

    /**
     * number of different items in the dataset
     */
    static int itemCount = 0;
    /**
     * total number of transactions in dataset
     */
    static int totalCount = 0;
    /**
     * minimum support for a frequent itemset in percentage, e.g. 0.8
     */
    static double support_min = 0.8;

    /**
     * 1     * PJMR job main program
     *
     * @param args Command line arguments
     */
    public void main(String[] args) {
        //Parse command line arguments.
        if (args.length < 1) usage();

        //We need to split the input data into n number of smaller files, where equals to the number of mappers
        //And the number of mappers should be equal to tbe number of cores*number of clusters
        // Configure mapper tasks.

        int NT = Math.max(threads(),1);

        for (String arg : args)
            mapperTask()
                    .source(new TextFileSource(arg))
                    .mapper(NT,AprioriMapper.class);

        // Configure reducer task.s
        reducerTask()
                .runInJobProcess()
                .reducer(AprioriReducer.class);
        //support_min=Double.parseDouble(args[1]);

        startJob();

    }


    /**
     * Print a usage message and exit.
     */
    private static void usage() {

        System.err.println("Usage: java pj2 [threads=<NT>] edu.rit.pjmr.example.WebLog01 <nodes> <file> [<pattern>]");
        terminate(1);
    }

    /**
     * Mapper class
     */
    private static class AprioriMapper extends Mapper<TextId, String, String, LongVbl> {

        private static final LongVbl ONE = new LongVbl.Sum(1L);

        public void map(TextId textId, //File name and line number
                        String contents, //Line from file
                        Combiner<String, LongVbl> combiner) {
            System.err.println("Hello from mapper");
            Scanner scanner = new Scanner(contents);
            while (scanner.hasNext()) {
                //For each number, compute:
                String s = scanner.next();
                combiner.add(s, ONE);
            }
        }
    }


    /**
     * Reducer Class
     */
    private static class AprioriReducer extends Reducer<String, LongVbl> {
        public void start(String[] args){
            itemsets = new CopyOnWriteArrayList<>();
            frequentCandidates = new CopyOnWriteArrayList<>();
        }
        public void reduce(String key, LongVbl value) {

            int[] cand = {Integer.parseInt(key)};
            itemsets.add(cand);
            totalCount++;
            itemCount = itemCount + (int)value.item;

            // if the count% is larger than the support_min%, add to the candidate to
            // the frequent candidates
            if(((int)value.item / (double) (totalCount)) >= support_min){
                System.out.println(key + "  (" + (((int)value.item / (double) totalCount))+" "+(int)value.item+")");
                frequentCandidates.add(cand);
            }
            //new candidates are only the frequent candidates
            itemsets = frequentCandidates;
            FrequentSets_count = 0;
            //System.err.println(frequentCandidates.size());
            //System.out.flush();
        }

        public void finish(){
            //System.err.println("hello from finish");
            //System.err.println(itemsets.size());
            //System.out.flush();
            if(itemsets.size()!=0){
                FrequentSets_count+=itemsets.size();
                System.err.println("Found "+itemsets.size()+" frequent itemsets of size " + itemsetNumber + " (with support "+(support_min*100)+"%)");;
                // by construction, all existing itemsets have the same size
                int currentSizeOfItemsets = itemsets.get(0).length;
                System.err.println("Creating itemsets of size "+(currentSizeOfItemsets+1)+" based on "+itemsets.size()+" itemsets of size "+currentSizeOfItemsets);

                ConcurrentHashMap<String, int[]> tempCandidates = new ConcurrentHashMap<>(); //temporary candidates

                // compare each pair of itemsets of size n-1
                for(int i=0; i<itemsets.size(); i++){
                    for(int j=i+1; j<itemsets.size(); j++){
                        int[] X = itemsets.get(i);
                        int[] Y = itemsets.get(j);

                        assert (X.length==Y.length);

                        //make a string of the first n-2 tokens of the strings
                        int [] newCand = new int[currentSizeOfItemsets+1];
                        System.arraycopy(X, 0, newCand, 0, newCand.length - 1);

                        int ndifferent = 0;
                        // then we find the missing value
                        for (int aY : Y) {
                            boolean found = false;
                            // is Y[s1] in X?
                            for (int aX : X) {
                                if (aX == aY) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) { // Y[s1] is not in X
                                ndifferent++;
                                // we put the missing value at the end of newCand
                                newCand[newCand.length - 1] = aY;
                            }

                        }

                        // we have to find at least 1 different, otherwise it means that we have two times the same set in the existing candidates
                        assert(ndifferent>0);


                        if (ndifferent==1){
                            // HashMap does not have the correct "equals" for int[] :-(
                            // I have to create the hash myself using a String :-(
                            // I use Arrays.toString to reuse equals and hashcode of String
                            Arrays.sort(newCand);
                            tempCandidates.put(Arrays.toString(newCand),newCand);

                        }
                    }
                }

                //set the new itemsets
                itemsets = new CopyOnWriteArrayList<>(tempCandidates.values());
                System.err.println("Created "+itemsets.size()+" unique itemsets of size "+(currentSizeOfItemsets+1));
                System.out.flush();
            }

            File file = new File("itemsets.dat");
            try {
                ObjectOutputStream output = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(file)));

                output.writeObject(itemsets);
                output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            int[] storeitem = new int[2];
            storeitem[0]=itemCount;
            storeitem[1]= totalCount;
            File file2 = new File("itemcount.dat");
            try {
                ObjectOutputStream output = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(file2)));

                output.writeObject(storeitem);
                output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

}