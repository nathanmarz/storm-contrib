package storm.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSUtils {
    public static List<Long> getSortedVersions(FileSystem fs, String dir, String suffix) {
        try {
            List<Long> ret = new ArrayList<Long>();
            FileStatus[] files = fs.listStatus(new Path(dir));
            for(FileStatus s: files) {
                String name = s.getPath().getName();
                if(name.endsWith(suffix)) {
                    String v = name.substring(0, name.length() - suffix.length());
                    ret.add(Long.parseLong(v));
                }
            }
            Collections.sort(ret);
            return ret;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void clearDir(FileSystem fs, String dir) {
        try {
            fs.delete(new Path(dir), true);
            fs.mkdirs(new Path(dir));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void mkdirs(FileSystem fs, String dir) {
        try {
            fs.mkdirs(new Path(dir));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void deleteFile(FileSystem fs, String path) {
        try {
            fs.delete(new Path(path), false);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
