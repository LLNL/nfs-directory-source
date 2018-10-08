package gov.llnl.sonar.kafka.connect.parsers;

import gov.llnl.sonar.kafka.connect.exceptions.ParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import scala.util.control.Exception;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;

import static java.nio.file.AccessMode.READ;

@Slf4j
public abstract class FileStreamParser {

    private FileChannel fileChannel = null;

    String filename;
    Schema avroSchema;
    String eofSentinel;
    long currentLine = -1l;

    abstract void init();
    public abstract Object readNextRecord() throws ParseException, EOFException;

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

        if (fileChannel == null) {
            throw new EOFException("Invalid fileChannel!");
        }

        buffer.clear();
        StringBuffer line = new StringBuffer();
        while(fileChannel.read(buffer) > 0)
        {
            buffer.flip();
            CharBuffer buf = StandardCharsets.ISO_8859_1.decode(buffer);
            char ch = buf.get(0);
            if (ch == '\n') {
                String lineString = line.toString();
                if (eofSentinel != null && lineString.equals(eofSentinel)) {
                    close();
                    throw new EOFException("EOF sentinel reached!");
                }
                currentLine++;
                return lineString;
            } else {
                line.append(ch);
            }
            buffer.clear();
        }
        if (line.length() != 0) {
            String lineString = line.toString();
            if (eofSentinel != null && lineString.equals(eofSentinel)) {
                close();
                throw new EOFException("EOF sentinel reached!");
            }
            currentLine++;
            return lineString;
        }

        close();
        throw new EOFException("End of fileChannel reached!");
    }

    public long offset() {
        try {
            return fileChannel.position();
        } catch (NullPointerException | ClosedChannelException e) {
            return -1L;
        } catch (IOException e) {
            log.error("IOException:", e);
        }
        return -1L;
    }
}
