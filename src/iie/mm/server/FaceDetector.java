package iie.mm.server;

import org.opencv.core.*;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class FaceDetector {
    private CascadeClassifier faceDetector = null;
    private boolean initialized = false;

    public FaceDetector(String confPath) {
        if (!initialized && confPath != null) {
            System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
            initialized = true;
            System.out.println("Running FaceDetector");
        }
        if (confPath != null && !confPath.equals(""))
            faceDetector = new CascadeClassifier(confPath);
    }

    public List<BufferedImage> detect(BufferedImage bi) {
        List<BufferedImage> r = new ArrayList<BufferedImage>();

        if (faceDetector == null)
            return r;

        byte[] pixels = ((DataBufferByte) bi.getRaster().getDataBuffer()).getData();

        // Create a Matrix the same size of image
        Mat image = new Mat(bi.getHeight(), bi.getWidth(), CvType.CV_8UC3);
        // Fill Matrix with image values
        image.put(0, 0, pixels);
        if (image.empty())
            return r;

        MatOfRect faceDetections = new MatOfRect();
        faceDetector.detectMultiScale(image, faceDetections);

        for (Rect rect : faceDetections.toArray()) {
            Mat face = new Mat(image, rect);
            MatOfByte mob = new MatOfByte();
            if (Highgui.imencode(".jpg", face, mob)) {
                byte[] bytes = mob.toArray();
                InputStream in = new ByteArrayInputStream(bytes);
                BufferedImage img = null;
                try {
                    img = ImageIO.read(in);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (img != null)
                    r.add(img);
            } else {
                System.out.println("Encode face failed.");
            }
        }

        return r;
    }

    public static void main(String[] args) throws IOException {

        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        System.out.println("Running FaceDetector");

        for (int i = 0; i < 10; i++) {
            do {
                System.out.print("I " + i + " Y or N?");
                int r = System.in.read();
                if (r == 'Y') {
                    System.in.skip(System.in.available());
                    break;
                } else if (r == 'N') {
                    System.in.skip(System.in.available());
                    break;
                }
            } while (true);
        }

        CascadeClassifier faceDetector = new CascadeClassifier("/home/macan/Downloads/OPENCV/opencv-2.4.9/data/haarcascades/haarcascade_frontalface_alt.xml");
        Mat image = Highgui.imread("/home/macan/workspace/dservice/data/images/train9.jpg");
        if (image.empty()) {
            System.out.println("Empty figure?!");
            System.exit(0);
        }

        MatOfRect faceDetections = new MatOfRect();
        faceDetector.detectMultiScale(image, faceDetections);

        System.out.println(String.format("Detected %s faces", faceDetections.toArray().length));

        for (Rect rect : faceDetections.toArray()) {
            Core.rectangle(image, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height),
                    new Scalar(0, 255, 0));
        }

        String filename = "/home/macan/workspace/dservice/data/faces/ouput.png";
        System.out.println(String.format("Writing %s", filename));
        Highgui.imwrite(filename, image);

        int i = 0;
        for (Rect rect : faceDetections.toArray()) {
            Mat face = new Mat(image, rect);
            String fn = "/home/macan/workspace/dservice/data/faces/ouput_" + (i++) + ".png";
            System.out.println(String.format("Writing %s", fn));
            Highgui.imwrite(fn, face);
        }

        FaceDetector fd = new FaceDetector("/home/macan/Downloads/OPENCV/opencv-2.4.9/data/haarcascades/haarcascade_frontalface_alt.xml");
        try {
            List<BufferedImage> faces = fd.detect(ImageIO.read(new File("/home/macan/workspace/dservice/data/images/train1.jpg")));
            System.out.println("Got " + faces.size() + " faces.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
