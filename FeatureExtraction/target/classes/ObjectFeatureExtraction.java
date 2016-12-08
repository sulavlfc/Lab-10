import org.openimaj.feature.local.list.LocalFeatureList;
import org.openimaj.image.ImageUtilities;
import org.openimaj.image.MBFImage;
import org.openimaj.image.feature.local.engine.DoGSIFTEngine;
import org.openimaj.image.feature.local.keypoints.Keypoint;

import java.io.*;

/**
 * Created by Naga on 20-09-2016.
 */
public class ObjectFeatureExtraction {




    public static void main(String args[]) throws IOException {

        String inputFolder = "original data/";
        String outputFolder = "output/";


        File dir = new File(inputFolder);
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String filename)
            { return filename.endsWith(".jpg"); }
        } );
        FileWriter fw = new FileWriter((outputFolder +"features.txt"),true);
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i=0; i<files.length;i++) {
            System.out.println(files[i]);
            MBFImage mbfImage = ImageUtilities.readMBF(new File(String.valueOf(files[i])));
            DoGSIFTEngine doGSIFTEngine = new DoGSIFTEngine();
            LocalFeatureList<Keypoint> features = doGSIFTEngine.findFeatures(mbfImage.flatten());


            for (int j = 0; j < features.size(); j++) {
                double c[] = features.get(j).getFeatureVector().asDoubleVector();
                bw.write(i + ",");
                for (int k = 0; k < c.length; k++) {
                    bw.write(c[k] + " ");
                }
                bw.newLine();
            }


        }
        bw.close();
    }

        //File[] files = finder(inputFolder);
        //System.out.println(files);
      /*  String inputImage = "sealion.jpg";
        String outputFolder = "output/";
        String[] IMAGE_CATEGORIES = {"Horse", "Bird", "SeaLion", "Swan", "Beaver"};
        int input_class = 2;
        MBFImage mbfImage = ImageUtilities.readMBF(new File(inputFolder + inputImage));
        DoGSIFTEngine doGSIFTEngine = new DoGSIFTEngine();
        LocalFeatureList<Keypoint> features = doGSIFTEngine.findFeatures(mbfImage.flatten());
        System.out.println(input_class);
        FileWriter fw = new FileWriter(outputFolder + IMAGE_CATEGORIES[input_class] + ".txt");
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 0; i < features.size(); i++) {
            double c[] = features.get(i).getFeatureVector().asDoubleVector();
            bw.write(input_class + ",");
            for (int j = 0; j < c.length; j++) {
                bw.write(c[j] + " ");
            }
            bw.newLine();
        }
        bw.close();*/
    }

