package helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/** The HDFSReader class is a helper class for reading the output of a MapReduce job from HDFS. */
public class HDFSReader {
    public static String[] read(Configuration conf, String outputDir) throws IOException {
        List<String> pickedLines = new LinkedList<String>();
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] files = hdfs.listStatus(new Path(outputDir));
        for (FileStatus file : files) {
            if (!file.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(file.getPath())));
                String line;
                while ((line = br.readLine()) != null){
                    pickedLines.add(line);
                }
                br.close();
            }
        }
        return pickedLines.toArray(new String[0]);
    }
}
