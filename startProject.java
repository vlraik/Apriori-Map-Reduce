//package apriori;
import edu.rit.pj2.vbl.LongVbl;
import java.io.*;
import java.lang.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.GZIPInputStream;

public class startProject {
    private static CopyOnWriteArrayList<int[]> itemsets;
    public static void main(String[] args) throws IOException {
        String[] array = "AprioriMR accidents_1.txt accidents_2.txt".split(" ");
        pj2.main(array);

        String[] array2 = "AprioriMR2 accidents_1.txt accidents_2.txt".split(" ");

        String[] array3 = "AprioriMR3 abc.txt".split(" ");
        //pj2.main(array3);
        //Read the itemset file
        File file = new File("itemsets.dat");
        ObjectInputStream input = new ObjectInputStream(new GZIPInputStream(new FileInputStream(file)));

        //Read the object
        try {
            Object readObject = null;
            try {
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

        while(itemsets.size()>0) {

            //System.err.println(System.getProperty("user.dir"));
            //System.out.flush();
            pj2.main(array2);
        }

    }
}
