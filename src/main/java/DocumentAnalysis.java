import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.orc.OrcProto;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions.*;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DocumentAnalysis {

    static SparkConf sparkConf;
    static SparkSession spark;
    static SQLContext sqlContext;
    static JavaSparkContext jsc;

    public static void main(String[] args){

        // Disable logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create a Java Spark Context.
        sparkConf = new SparkConf().setAppName("Amdocs - Tests").setMaster("local[*]");
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
        sqlContext = new SQLContext(spark.sparkContext());
        jsc = new JavaSparkContext(spark.sparkContext());

        processLogLine();

    }

    public static void processLogLine(){
        Dataset<LogFact> lines = null;

        try {

            lines = getLines();

        } catch (ParseException e) {
            e.printStackTrace();
        }

        Map<String, String> params = getParams();
        LinkedList<String> hierarchy = new LinkedList<String>();
        hierarchy.add("office");
        hierarchy.add("user");
        hierarchy.add("day");

        lines.show(false);

        System.out.println("\nSummary: ");
        getSummary(lines, params, "");


        System.out.println("\nDetail: ");
        getDetail(lines, params, hierarchy,"");

    }

    public static void getDetail( Dataset<LogFact> lines, Map<String, String> params, LinkedList<String> hierarchy, String prefix){


        if( !hierarchy.isEmpty() ){
            String elem = hierarchy.poll();

            if( params.containsKey( elem ) ){

                HashMap<String, String> temp = new HashMap<String, String>();
                temp.put(elem, params.get(elem));

                Dataset<LogFact> filtered = applyFilters(lines, temp);

                long count = filtered.select("docId").distinct().count();

                System.out.println(prefix+" "+elem+" "+params.get(elem)+":");
                System.out.println(prefix+" "+count+" elements analysed");

                getDetail( filtered, params, hierarchy, prefix+"\t");

            } else {
                Row[] rows = (Row[]) lines.select(elem).distinct().collect();

                for( Row r : rows ){
                    String value = r.getString(0);

                    HashMap<String, String> newParams = new HashMap<String, String>(params);
                    newParams.put(elem, value);
                    LinkedList<String> newHierarchy = new LinkedList<String>(hierarchy);
                    newHierarchy.addFirst(elem);

                    getDetail(lines, newParams, newHierarchy, prefix+"\t");
                }
            }
        } else {
            getSummary( lines, params, prefix+"\t");
        }
    }


    public static void getSummary( Dataset<LogFact> lines, Map<String, String> params, String prefix ){

        Dataset<LogFact> filtered = applyFilters(lines, params);

        WindowSpec wSpec = Window.partitionBy("docId").orderBy("time");
        Dataset<Row> ordered = filtered.withColumn("rank", functions.row_number().over(wSpec) );
        Dataset<Row> start = ordered.filter(ordered.col("rank").equalTo(1));
        Dataset<Row> joined = ordered.join( start.select("docId","time").withColumnRenamed("time", "startTime"), "docId" );
        Dataset<Row> diff = joined.withColumn("diff", functions.expr("time - startTime") );

        Dataset<Row> avg = diff.groupBy("rank").avg("diff");


        System.out.println(prefix+"Avrg time to scan:"+avg.filter(avg.col("rank").equalTo(2)).select("avg(diff)").first()+" ms" );
        System.out.println(prefix+"Avrg time to save img:"+avg.filter(avg.col("rank").equalTo(4)).select("avg(diff)").first()+" ms" );
        System.out.println(prefix+"Avrg time to show image:"+avg.filter(avg.col("rank").equalTo(6)).select("avg(diff)").first()+" ms" );
        System.out.println(prefix+"Avrg time to scan:"+avg.filter(avg.col("rank").equalTo(2)).select("avg(diff)").first()+" ms" );
        System.out.println(prefix+"Documents scanned:"+ diff.select("docId").distinct().count() );



    }

    public static Dataset<LogFact> applyFilters( Dataset<LogFact> lines, Map<String,String> filters){

        Dataset<LogFact> filtered = lines;

        if(filters.containsKey("day")){
            filtered = filtered.filter( lines.col("day").equalTo( Integer.parseInt( filters.get("day") ) ) );
        }

        if(filters.containsKey("office")){
            filtered = filtered.filter( lines.col("office").equalTo( filters.get("office") ) );
        }

        if(filters.containsKey("user")){
            filtered = filtered.filter( lines.col("user").equalTo(  filters.get("user") ) );
        }

        return filtered;
    }

    public static Map<String, String> getParams(){

        Map<String,String> params = new HashMap<String, String>();

//        params.put("day", "2");
//        params.put("hour", "14");
        params.put("office", "Genova");
//        params.put("user", "Paco");

        return  params;

    }

    public static Dataset<LogFact> getLines() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss.SSS");

        List<LogFact> logs = new ArrayList<LogFact>();

        logs.add( new LogFact(1,"Paris", "Carlos", "2", sdf.parse("14:27:30.646"), "*********Starting scan********" ) );
        logs.add( new LogFact(1,"Paris", "Carlos", "2", sdf.parse("14:28:30.646"), "Scan done. Image loaded in memory" ) );
        logs.add( new LogFact(1,"Paris", "Carlos", "2", sdf.parse("14:29:30.646"), "Saving sample TIF image in share disc ..." ) );
        logs.add( new LogFact(1,"Paris", "Carlos", "2", sdf.parse("14:32:30.646"), "Image showed in applet" ) );
        logs.add( new LogFact(1,"Paris", "Carlos", "2", sdf.parse("14:30:30.646"), "Image TIF saved in shared disc" ) );
        logs.add( new LogFact(1,"Paris", "Carlos", "2", sdf.parse("14:31:30.646"), "Loading image... " ) );

        logs.add( new LogFact(2,"Genova", "Paco", "2", sdf.parse("14:27:30.646"), "*********Starting scan********" ) );
        logs.add( new LogFact(2,"Genova", "Paco", "2", sdf.parse("14:27:32.646"), "Scan done. Image loaded in memory" ) );
        logs.add( new LogFact(2,"Genova", "Paco", "2", sdf.parse("14:27:35.646"), "Saving sample TIF image in share disc ..." ) );
        logs.add( new LogFact(2,"Genova", "Paco", "2", sdf.parse("14:27:40.646"), "Image TIF saved in shared disc" ) );
        logs.add( new LogFact(2,"Genova", "Paco", "2", sdf.parse("14:27:50.646"), "Loading image... " ) );
        logs.add( new LogFact(2,"Genova", "Paco", "2", sdf.parse("14:28:00.646"), "Image showed in applet" ) );

        logs.add( new LogFact(3,"Genova", "Jose", "5", sdf.parse("14:27:20.646"), "*********Starting scan********" ) );
        logs.add( new LogFact(3,"Genova", "Jose", "5", sdf.parse("14:28:30.646"), "Scan done. Image loaded in memory" ) );
        logs.add( new LogFact(3,"Genova", "Jose", "5", sdf.parse("14:29:40.646"), "Saving sample TIF image in share disc ..." ) );
        logs.add( new LogFact(3,"Genova", "Jose", "5", sdf.parse("14:30:50.646"), "Image TIF saved in shared disc" ) );
        logs.add( new LogFact(3,"Genova", "Jose", "5", sdf.parse("14:31:55.646"), "Loading image... " ) );
        logs.add( new LogFact(3,"Genova", "Jose", "5", sdf.parse("14:32:30.646"), "Image showed in applet" ) );

        logs.add( new LogFact(4,"Genova", "Rick", "2", sdf.parse("14:26:30.646"), "*********Starting scan********" ) );
        logs.add( new LogFact(4,"Genova", "Rick", "2", sdf.parse("14:27:32.646"), "Scan done. Image loaded in memory" ) );
        logs.add( new LogFact(4,"Genova", "Rick", "2", sdf.parse("14:28:35.646"), "Saving sample TIF image in share disc ..." ) );
        logs.add( new LogFact(4,"Genova", "Rick", "2", sdf.parse("14:29:40.646"), "Image TIF saved in shared disc" ) );
        logs.add( new LogFact(4,"Genova", "Rick", "2", sdf.parse("14:30:50.646"), "Loading image... " ) );
        logs.add( new LogFact(4,"Genova", "Rick", "2", sdf.parse("14:31:00.646"), "Image showed in applet" ) );

        logs.add( new LogFact(5,"Genova", "Paco", "2", sdf.parse("13:26:30.646"), "*********Starting scan********" ) );
        logs.add( new LogFact(5,"Genova", "Paco", "2", sdf.parse("13:27:32.646"), "Scan done. Image loaded in memory" ) );
        logs.add( new LogFact(5,"Genova", "Paco", "2", sdf.parse("13:28:35.646"), "Saving sample TIF image in share disc ..." ) );
        logs.add( new LogFact(5,"Genova", "Paco", "2", sdf.parse("13:29:40.646"), "Image TIF saved in shared disc" ) );
        logs.add( new LogFact(5,"Genova", "Paco", "2", sdf.parse("13:30:50.646"), "Loading image... " ) );
        logs.add( new LogFact(5,"Genova", "Paco", "2", sdf.parse("13:31:00.646"), "Image showed in applet" ) );


        Encoder<LogFact> logFactEncoder = Encoders.bean(LogFact.class);
        return sqlContext.createDataset( logs, logFactEncoder );
    }

}
