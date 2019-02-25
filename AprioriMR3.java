//package apriori;

//import edu.rit.pjmr.PjmrJob;

import edu.rit.pj2.vbl.LongVbl;

import static edu.rit.pj2.Task.terminate;

public class AprioriMR3 extends PjmrJob2<TextId, String, String, LongVbl> {

    public void main(String[] args){

        if(args.length<1) usage();

        for (int i =0;i<1;i++) {
            System.err.println("Inside the main for loop");


            for (String arg : args)
                mapperTask().source(new TextFileSource(arg)).mapper(AprioriMapper.class);

            reducerTask()
                    .runInJobProcess()
                    .reducer(AprioriReducer.class);

            startJob();
            System.out.flush();
            restartJob();
        }
        //mapperTask().source(new TextFileSource("abc.txt")).mapper(AprioriMapper2.class);
        //reducerTask().reducer(AprioriReducer2.class);
        //startJob();
        //restartJob();
    }

    /**
     * Print a usage message and exit.
     */
    private static void usage(){

        System.err.println ("Usage: java pj2 [threads=<NT>] edu.rit.pjmr.example.WebLog01 <nodes> <file> [<pattern>]");
        terminate (1);
    }

    private static class AprioriMapper extends Mapper<TextId, String, String, LongVbl>{
        private static final LongVbl example = new LongVbl.Sum(1);
        public void map(TextId textId, String contents, Combiner<String, LongVbl> combiner){
            System.err.println("Hello from the mapper");
            //System.out.flush();
            combiner.add("abc", example);
        }
    }

    private static class AprioriMapper2 extends Mapper<TextId, String, String, LongVbl>{
        private static final LongVbl example = new LongVbl.Sum(1);
        public void map(TextId textId, String contents, Combiner<String, LongVbl> combiner){
            System.err.println("Hello from the mapper2");
            //System.out.flush();
            combiner.add("abc", example);
        }
    }

    private static class AprioriReducer extends Reducer<String, LongVbl>{
        public void reduce(String key, LongVbl value){
            System.out.println("Hellow from the reducer");
            //System.out.flush();
        }
        public void finish(){
            System.err.println("Hello from the finish");
        }
    }

    private static class AprioriReducer2 extends Reducer<String, LongVbl>{
        public void reduce(String key, LongVbl value){
            System.out.println("Hellow from the reducer2");
            //System.out.flush();
        }

    }

}