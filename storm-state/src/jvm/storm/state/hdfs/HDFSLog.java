package storm.state.hdfs;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

public class HDFSLog {
    public static final Logger LOG = Logger.getLogger(HDFSLog.class);
    
    public static LogWriter create(FileSystem fs, String path, Kryo kryo) {
        return new LogWriter(fs, path, kryo);
    }

    public static LogReader open(FileSystem fs, String path, Kryo kryo) {
        return new LogReader(fs, path, kryo);
    }
    
    public static class LogWriter {
        SequenceFile.Writer _writer;
        Kryo _kryo;
        BytesWritable _key = new BytesWritable();
        Output _output = new Output(2000, 20000000);

        protected LogWriter(FileSystem fs, String path, Kryo kryo) {
            try {
                _kryo = kryo;
                _writer = SequenceFile.createWriter(fs, fs.getConf(), new Path(path), BytesWritable.class, NullWritable.class, SequenceFile.CompressionType.NONE);
                _writer.syncFs();
            } catch(IOException e) {
                throw new RuntimeException(e);
            }            
        }
        
        /**
         * Returns the amount of data written (in bytes).
         * 
         */
        public int write(Object o) {
            _output.clear();
            _kryo.writeClassAndObject(_output, o);
            int amtWritten = _output.total();
            _key.set(_output.getBuffer(), 0, amtWritten);
            try {
                LOG.info("Writing to log: " + o);
                _writer.append(_key, NullWritable.get());
                return amtWritten;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
        }
        
        public void sync() {
            try {
                //LOG.info("Syncing to fs");
                //this doesn't work in local mode...?
                // this doesn't work on the cluster either...
                _writer.syncFs();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        public void close() {
            try {
                _writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class LogReader {
        SequenceFileReader _reader = null;
        BytesWritable _key = new BytesWritable();
        Kryo _kryo;
        Input _input = new Input(0);
        String _path;
        FileSystem _fs;
        long _amtRead;
        
        protected LogReader(FileSystem fs, String path, Kryo kryo) {
            _kryo = kryo;
            _fs = fs;
            _path = path;
        }
        
        public long amtRead() {
            return _amtRead;
        }
        
        //returns null and automatically closes itself when there's nothing left
        public Object read() {
            try {
                if(_reader==null) {
                    _reader = new SequenceFileReader(_fs, new Path(_path), _fs.getConf());
                }
                boolean gotnew = _reader.next(_key, NullWritable.get());
                if(!gotnew) {
                    _reader.close();
                    return null;
                } else {
                    int keyLength = _key.getLength();
                    _amtRead += keyLength;
                    _input.setBuffer(_key.getBytes(), 0, keyLength);
                    
                    return _kryo.readClassAndObject(_input);
                }
            } catch(EOFException e) {
                LOG.warn("Txlog is corrupt, skipping the rest of it (writer may have crashed in middle of writing", e);
                return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }        
    }
}
