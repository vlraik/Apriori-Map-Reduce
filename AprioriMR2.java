//package apriori;

//import edu.rit.pjmr.PjmrJob;

import edu.rit.pj2.vbl.LongVbl;

import static edu.rit.pj2.Task.terminate;
import edu.rit.pjmr.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class AprioriMR2 extends PjmrJob<TextId, String, int[], LongVbl> {

    static CopyOnWriteArrayList<int[]> itemsets;
    static CopyOnWriteArrayList<int[]> frequentCandidates= new CopyOnWriteArrayList<>();;
    static boolean[] trans;
    static int FrequentSets_count=0;
    static int itemsetNumber=1;

    /**
     *number of different items in the dataset
     */
    static int itemCount = 0;
    /** total number of transactions in dataset */
    static int totalCount = 0;
    /** minimum support for a frequent itemset in percentage, e.g. 0.8 */
    static double support_min = 0.8;

    static boolean flag = true;

    public void main(String[] args){

        if(args.length<1) usage();


        System.err.println("Inside the main for loop");

        //Read the itemset file
        File file = new File("itemsets.dat");
        ObjectInputStream input = null;
        try {
            input = new ObjectInputStream(new GZIPInputStream(new FileInputStream(file)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Read the object
        try {
            Object readObject = null;
            try {
                assert input != null;
                readObject = input.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            input.close();
            if (!(readObject instanceof CopyOnWriteArrayList)) throw new IOException("Data is wrong");

            itemsets=(CopyOnWriteArrayList<int[]>) readObject;
        }
        catch (IOException e){
            e.printStackTrace();
        }


        //Read the itemcount file
        File file2 = new File("itemcount.dat");
        ObjectInputStream input2 = null;
        try {
            input = new ObjectInputStream(new GZIPInputStream(new FileInputStream(file2)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Read the object
        try {
            Object readObject = null;
            try {
                assert input2 != null;
                readObject = input.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            input.close();
            if (!(readObject instanceof int[])) throw new IOException("Data is wrong");

            int[] storeitem=(int[]) readObject;
            itemCount = storeitem[0];
            totalCount = storeitem[1];
        }
        catch (IOException e){
            e.printStackTrace();
        }



        //Determine the number of mapper threads
        int NT = Math.max(threads(),1);

        for (String arg : args)
            mapperTask()
                    .source(new TextFileSource(arg))
                    .mapper(NT,AprioriMapper.class);

        reducerTask()
                .runInJobProcess()
                .reducer(AprioriReducer.class);

        startJob();

    }

    /**
     * Print a usage message and exit.
     */
    private static void usage(){

        System.err.println ("Usage: java pj2 [threads=<NT>] edu.rit.pjmr.example.WebLog01 <nodes> <file> [<pattern>]");
        terminate (1);
    }

    /**
     * Mapper task for the apriori algorithm
     */
    private static class AprioriMapper extends Mapper<TextId, String, int[], LongVbl>{

        private static final LongVbl ONE = new LongVbl.Sum(1L);

        public void map(TextId textId, //File name and line number
                        String contents, //Line from file
                        Combiner<int[], LongVbl> combiner) {
            //Scanner scanner = new Scanner(contents);   trans = new boolean[itemCount];
            trans = new boolean[itemCount];
            Arrays.fill(trans, false);
            StringTokenizer stFile = new StringTokenizer(contents, " "); //read a line from the file to the tokenizer
            //put the contents of that line into the transaction array
            while(stFile.hasMoreTokens()){

                int parsedVal = Integer.parseInt(stFile.nextToken());
                trans[parsedVal]=true; //if it is not a 0, assign the value to true
            }

            // check each candidate
            for (int[] itemset : itemsets) {
                boolean match = true; // reset match to false
                // tokenize the candidate so that we know what items need to be
                // present for a match

                for (int xx : itemset) {
                    if (!trans[xx]) {
                        match = false;
                        break;
                    }
                }
                if (match) { // if at this point it is a match, increase the count
                    combiner.add(itemset, ONE);
                }
            }
        }
    }

    /**
     * Reducer task for the apriori algorithm
     */
    private static class AprioriReducer extends Reducer<int[], LongVbl>{
        public void start(){
            frequentCandidates = new CopyOnWriteArrayList<>();
            System.out.println("Inside the start of the reducer");
            System.out.flush();
        }
        public void reduce(int[] key, LongVbl value){
            // the frequent candidates
            if (((int) value.item / (double) (totalCount)) >= support_min) {
                System.out.println(Arrays.toString(key) + "  (" + (((int) value.item / (double) totalCount)) + " " + (int) value.item + ")");
                System.out.println(Arrays.toString(key));
                frequentCandidates.add(key);
            }

            itemsets = frequentCandidates;
            FrequentSets_count = 0;

        }
        public void finish(){
            if(itemsets.size()!=0){
                FrequentSets_count+=itemsets.size();
                System.out.println("Found "+itemsets.size()+" frequent itemsets of size " + itemsetNumber + " (with support "+(support_min*100)+"%)");;
                // by construction, all existing itemsets have the same size
                int currentSizeOfItemsets = itemsets.get(0).length;
                System.out.println("Creating itemsets of size "+(currentSizeOfItemsets+1)+" based on "+itemsets.size()+" itemsets of size "+currentSizeOfItemsets);

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
                System.out.println("Created "+itemsets.size()+" unique itemsets of size "+(currentSizeOfItemsets+1));
            }

            File file = new File("itemsets.dat");
            try {
                ObjectOutputStream output = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(file)));

                output.writeObject(itemsets);
                output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}