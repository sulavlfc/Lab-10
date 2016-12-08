package dtParsing;

import com.google.common.base.Joiner;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Naga on 02-11-2016.
 */
public class ParsingNew {

    public static void main(String args[]) throws IOException {
        String label = "2.0";
        String inputPath = "tree.txt"; //Input path of decision tree model from Spark-ML-Lib
        String outputPath = "Data/Class2.txt"; //Output path for each class label
        GeneratePathForClass(inputPath, outputPath, label);
    }

    public ParsingNew(String model, String outputPath, String label){
        try {
            GeneratePathForClass(model, outputPath, label);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void GeneratePathForClass(String model, String outputPath, String label) throws IOException {
        Boolean check = false;
        List<String> tree = new ArrayList<String>();
        String[] modelArray = model.toString().split("\n");
        for(int i=0; i<modelArray.length;i++) {

            if (modelArray[i].contains("return")) {

                if (modelArray[i].contains("return " + label + ";")) {
                    String labelChange = modelArray[i].replaceAll(label, "true");
                    tree.add(labelChange);
                }


                else {
                    String newStr = modelArray[i].substring(0, modelArray[i].indexOf("n"));
                    String t = newStr + "n false;";
                    tree.add(t);
                }
            }

            else {
                tree.add(modelArray[i]);
            }
        }


        Joiner joiner = Joiner.on("\n").useForNull("null");
        String output = joiner.join(tree);


        /*
        Saving output to a File
         */
        FileWriter fw = new FileWriter(new File(outputPath));
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(output);
        bw.close();
    }
}
