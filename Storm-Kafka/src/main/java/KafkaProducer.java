
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
public class KafkaProducer {
    private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public KafkaProducer() {
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        properties.put("message.max.bytes", "10000000");
        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }


    public static String EncodeVideo(String file) {
        String encodedString = null;
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
        } catch (Exception e) {
            // TODO: handle exception
        }
        byte[] bytes;
        byte[] buffer = new byte[8192];
        int bytesRead;


        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        bytes = output.toByteArray();
        encodedString = Base64.encodeToString(bytes, true);
        return encodedString;
    }

    public static void main(String[] args) {
        new KafkaProducer(); //Setting properties for kafka producer
        String topic = args[0];  //Topic Name
        String msg = EncodeVideo(args[1]); //Encoding the Video
        String outputfolder = "out/";
        //FileWriter fz = new FileWriter("abc.txt");

        //FileWriter fw = new FileWriter(outputfolder+"file.txt");
        String inputFolder = "data/";
        String inputImage = "horse1.jpg";

        //String[] IMAGE_CATEGORIES = {"Horse", "Bird", "SeaLion", "Swan", "Beaver"};
        int input_class = 2;
        MBFImage mbfImage;

        try {

            File f = new File(inputFolder + inputImage);
            mbfImage = ImageUtilities.readMBF(f);
            DoGSIFTEngine doGSIFTEngine = new DoGSIFTEngine();
            LocalFeatureList<Keypoint> features = doGSIFTEngine.findFeatures(mbfImage.flatten());
            FileWriter fz = new FileWriter("myfile.txt");
            FileWriter fw = new FileWriter(outputfolder + "newfeatures.txt");
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 0; i < features.size(); i++) {
                double c[] = features.get(i).getFeatureVector().asDoubleVector();
                //bw.write(input_class + ",");
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

/*
        System.out.println(input_class);


        Iterable<String> result = Splitter.fixedLength(100000).split(msg); //Splitting the video file
        String[] parts = Iterables.toArray(result, String.class); //Parts of video file
        for(int i=0; i<parts.length; i++){
            KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, parts[i]);
            System.out.println(parts[i]);

        }
        System.out.println("Video Sent");
        producer.close();
    }*/



