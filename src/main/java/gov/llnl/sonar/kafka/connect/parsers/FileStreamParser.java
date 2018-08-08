package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import scala.util.control.Exception;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;

import static java.nio.file.AccessMode.READ;

@Slf4j
public abstract class FileStreamParser extends Reader {

    String filename;
    FileChannel fileChannel = null;
    Schema avroSchema;
    String eofSentinel;


    abstract void init();
    public abstract Object readNextRecord() throws ParseException, EOFException;

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(len);
        int ret = fileChannel.read(byteBuffer);

        CharBuffer chars = StandardCharsets.ISO_8859_1.decode(byteBuffer);
        System.arraycopy(chars.array(), 0, cbuf, off, len);

        return ret;
    }

    public void close() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
            fileChannel = null;
        }
    }

    public synchronized void seekToOffset(Long offset) throws IOException {
        fileChannel.position(offset);
    }

    FileStreamParser(String filename, Schema avroSchema, String eofSentinel) {
        this.avroSchema = avroSchema;
        this.eofSentinel = eofSentinel;

        try {
            fileChannel = FileChannel.open(Paths.get(filename));
        } catch (IOException e) {
            log.error("IOException:", e);
        }
    }

    ByteBuffer buffer = ByteBuffer.allocate(1);

    protected String nextLine() throws IOException {
        StringBuffer line = new StringBuffer();
        while(fileChannel.read(buffer) > 0)
        {
            buffer.flip();
            for (int i = 0; i < buffer.limit(); i++)
            {
                char ch = ((char) buffer.get());
                if (ch == '\n') {
                    return line.toString();
                } else {
                    line.append(ch);
                }
            }
            buffer.clear();
        }
        throw new EOFException("End of fileChannel reached!");
    }

    public long offset() {
        try {
            return fileChannel.position();
        } catch (IOException e) {
            log.error("IOException:", e);
        }
        return -1L;
    }
}
