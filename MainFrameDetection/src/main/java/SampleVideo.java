/**
 * Created by sulav on 9/9/16.
 */
import org.openimaj.image.Image;
import org.openimaj.image.MBFImage;
import org.openimaj.video.Video;
import org.openimaj.video.VideoDisplay;
import org.openimaj.image.DisplayUtilities;
import org.openimaj.video.xuggle.XuggleVideo;
import org.openimaj.image.ImageUtilities;

import org.openimaj.image.feature.local.engine.DoGSIFTEngine;

import java.io.File;
import javax.imageio.ImageIO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.awt.image.BufferedImage;


public class SampleVideo {
    static Video<MBFImage> video;
    //    VideoDisplay<MBFImage> display = VideoDisplay.createVideoDisplay(video);
    static List<MBFImage> imageList = new ArrayList<MBFImage>();
    static List<Long> timeStamp = new ArrayList<Long>();
    static List<Double> mainPoints = new ArrayList<Double>();
    static int count;
    public static void main(String args[]) {
        String path = "data/sample.mkv";


        video = new XuggleVideo(new File(path));
        //VideoDisplay<MBFImage> display = VideoDisplay.createVideoDisplay(video);
        for (MBFImage mbfImage : video) {
            //DisplayUtilities.displayName(mbfImage.process(new CannyEdgeDetector()), "videoFrames");
            //DisplayUtilities.display(mbfImage);
            count += 1;
            //DisplayUtilities.display(mbfImage);
            String name = "Opt/frames/abc" +count + ".jpg";
            File outputFile = new File(name);
            BufferedImage bufferedFrame = ImageUtilities.createBufferedImageForDisplay(mbfImage);

            try {

                ImageIO.write(bufferedFrame, "jpg", outputFile);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.print(count);

        //video1= new XuggleVideo(new File(path));
    }
}
