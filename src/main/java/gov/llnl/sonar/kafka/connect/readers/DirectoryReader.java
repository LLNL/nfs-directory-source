package gov.llnl.sonar.kafka.connect.readers;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;


@Slf4j
public class DirectoryReader extends Reader {
    private String taskID;
    private String canonicalDirname;
    private String completedDirectoryName;
    private Path dirPath;

    private org.apache.avro.Schema avroSchema;
    private Long batchRows;
    private Long batchFiles;
    private String partitionField;
    private String offsetField;
    private String format;
    private Map<String, Object> formatOptions;

    private FileOffsetManager fileOffsetManager;

    public DirectoryReader(String dirname,
                           String completedDirectoryName,
                           org.apache.avro.Schema avroSchema,
                           Long batchRows,
                           Long batchFiles,
                           String partitionField,
                           String offsetField,
                           String format,
                           Map<String, Object> formatOptions,
                           String zooKeeperHost,
                           String zooKeeperPort)
            throws IOException {

        this.taskID = InetAddress.getLocalHost().getHostName() + "(" + Thread.currentThread().getId() + ")";
        this.completedDirectoryName = completedDirectoryName;
        this.avroSchema = avroSchema;
        this.batchRows = batchRows;
        this.batchFiles = batchFiles;
        this.partitionField = partitionField;
        this.offsetField = offsetField;
        this.format = format;
        this.formatOptions = formatOptions;

        File dir = new File(dirname);
        dirPath = dir.toPath();
        canonicalDirname = dir.getCanonicalPath();

        if (Files.notExists(dirPath)) {
            throw new FileNotFoundException();
        }

        Boolean isFolder = (Boolean) Files.getAttribute(dirPath, "basic:isDirectory");

        if (!isFolder) {
            throw new IOException(canonicalDirname + " is not a directory!");
        }

        try {
            this.fileOffsetManager = new FileOffsetManager(zooKeeperHost, zooKeeperPort, dirname, false);
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }
    }

    private List<FileReader> getNextFileReaders() {

        List<FileReader> readers = new ArrayList<>();

        try {

            // Walk through all files in dir
            Iterator<Path> pathWalker = Files.walk(dirPath).filter(Files::isRegularFile).iterator();

            fileOffsetManager.lock();

            while (readers.size() < batchFiles && pathWalker.hasNext()) {

                // Next file in dir
                Path p = pathWalker.next().toAbsolutePath();

                // Get and lock file offset if it is available
                final FileOffset fileOffset = fileOffsetManager.downloadFileOffsetWithLock(p.toString());

                // If not locked or completed, lock it and create a reader
                if (fileOffset != null) {
                    try {
                        FileReader newFileReader = new FileReader(
                                p,
                                completedDirectoryName,
                                avroSchema,
                                batchRows,
                                partitionField,
                                offsetField,
                                format,
                                formatOptions,
                                fileOffset.offset);

                        readers.add(newFileReader);

                        log.debug("Task {}: Created file reader for {}: {}", taskID, p.toString(), fileOffset);

                    } catch (Exception e) {
                        log.error("Task {}: {}", taskID, e);
                    }
                }
            }
        } catch (UncheckedIOException | NoSuchFileException e) {
            // Don't care about NoSuchFileException, that's just NFS catching up
            if (!(ExceptionUtils.getRootCause(e) instanceof NoSuchFileException)) {
                log.error("Task {}: {}", taskID, e);
            }
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }

        fileOffsetManager.unlock();

        return readers;
    }

    @Override
    public synchronized Long read(List<RawRecord> records, SourceTaskContext context) {

        Long numRecords = 0L;

        List<FileReader> currentFileReaders = getNextFileReaders();

        try {

            // Read from all file readers
            for (FileReader currentFileReader : currentFileReaders) {
                numRecords += currentFileReader.read(records, context);
                currentFileReader.close();
            }

            // Update and possibly purge all read files
            for (FileReader currentFileReader : currentFileReaders) {

                // Update file offset
                FileOffset currentFileOffset = new FileOffset(
                        currentFileReader.getCurrentOffset(),
                        false,
                        currentFileReader.ingestCompleted);

                // Upload updated offset
                fileOffsetManager.uploadFileOffset(currentFileReader.getPath().toString(), currentFileOffset);
                log.debug("Task {}: Uploaded file offset {}: {}", taskID, currentFileReader.getPath(), currentFileOffset);

                // Purge the file if completed
                if (currentFileReader.ingestCompleted) {
                    log.debug("Task {}: Purging file {}", taskID, currentFileReader.getPath().toString());
                    currentFileReader.purgeFile();
                }
            }

        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }

        return numRecords;
    }

    public String getCanonicalDirname() {
        return canonicalDirname;
    }

    public synchronized void close() {
        try {
            fileOffsetManager.close();
        } catch (Exception e) {
            log.error("Task {}: {}", taskID, e);
        }
    }

}
