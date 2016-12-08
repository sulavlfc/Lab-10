import dtParsing.ParsingNew;
import org.json.JSONObject;
import topology.CreateBolt;
import topology.CreateStromMainClass;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;


/**
 * Created by Naga on 27-10-2016.
 */


import dtParsing.ParsingNew;
import jdk.nashorn.internal.parser.JSONParser;
import org.json.JSONObject;
import topology.CreateBolt;
import topology.CreateStromMainClass;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

/**
 * Created by Naga on 27-10-2016.
 */
public class MainClass {



    public static void main(String args[]) throws IOException {

        String Path = "/media/sulav/Work/umkc college/Fall 2016/Real Time Big Data/Project/Storm-Kafka/src/main/java/";
        String line = null;
        StringBuilder sb1 = new StringBuilder();

        FileReader fileReader =
                new FileReader("tree.txt");

        // Always wrap FileReader in BufferedReader.
        BufferedReader bufferedReader =
                new BufferedReader(fileReader);

       /* while((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
        }*/

        if (bufferedReader != null) {
            int cp;

            while ((cp = bufferedReader.read()) != -1) {
                //System.out.println(cp);
                sb1.append((char) cp);
            }
            bufferedReader.close();
        }

        // Always close files.
        System.out.println(sb1.toString());
        String model = sb1.toString();
        ParsingNew parsing1 = new ParsingNew(model, "data/Class1.txt", "1.0");
        ParsingNew parsing2 = new ParsingNew(model, "data/Class2.txt", "2.0");


        System.out.println("Derived Class Paths");
        String[] bolts = {"Class1", "Class2",};
        String[] classDTpaths = {"data/Class1.txt", "data/Class2.txt"};


        //Create Bolt Class files
        for(int i=0; i<bolts.length; i++){
            CreateBolt createBolt = new CreateBolt(Path, bolts[i], classDTpaths[i]);
        }

        //Creating Storm Main Class


        String spoutName = "kafka_spout_audioFeatures";
        StringBuilder sb = new StringBuilder();
        String spout = "        topology.setSpout(\"" + spoutName+ "\", new KafkaSpout(kafkaConf), 4);";
        sb.append(spout).append("\n");
        for(int i=0; i<bolts.length; i++){
            String boltName = bolts[i] + "Bolt";
            String s2 = "        topology.setBolt(\"" + bolts[i] + "\", new " +boltName +"(), 4).shuffleGrouping(\"" + spoutName+ "\");";
            sb.append(s2);
            sb.append("\n");
        }


        CreateStromMainClass createStromMainClass = new CreateStromMainClass(sb.toString(), Path);

        System.out.println("Created Storm Topology");
    }
}
