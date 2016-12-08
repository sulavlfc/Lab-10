
import java.io.*;
import java.util.Properties;

/**
 * Created by sulav on 12/3/16.
 */



import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.migcomponents.migbase64.Base64;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.openimaj.feature.local.list.LocalFeatureList;
import org.openimaj.image.ImageUtilities;
import org.openimaj.image.MBFImage;
import org.openimaj.image.feature.local.engine.DoGSIFTEngine;
import org.openimaj.image.feature.local.keypoints.Keypoint;
import  java.io.*;
public class SimpleProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public SimpleProducer() {
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        properties.put("message.max.bytes", "10000000");
        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }
        public static void main(String[] args) {
        new SimpleProducer(); //Setting properties for kafka producer
        String topic = args[0];  //Topic Name

        String outputfolder = "out/";
        String inputFolder = "data/";
        String inputImage = "horse1.jpg";

        int input_class = 2;
        MBFImage mbfImage;

        try {

            File f = new File(inputFolder + inputImage);
            System.out.println(f);
            mbfImage = ImageUtilities.readMBF(f);
            DoGSIFTEngine doGSIFTEngine = new DoGSIFTEngine();
            LocalFeatureList<Keypoint> features = doGSIFTEngine.findFeatures(mbfImage.flatten());
            FileWriter fw = new FileWriter(outputfolder + "newfeatures.txt");
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 0; i < features.size(); i++) {
                double c[] = features.get(i).getFeatureVector().asDoubleVector();
                for (int j = 0; j < c.length; j++) {
                    //double a = c[j];
                    String data1 = String.valueOf(c[j]);
                    bw.write(c[j] + " ");
                    KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, data1);
                    producer.send(data);
                    System.out.println(data);
                }
                bw.newLine();
            }
            producer.close();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}





