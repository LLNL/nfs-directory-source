package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LLNLDirectorySourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(LLNLDirectorySourceTask.class);
    private String TAG = getClass().getName() + ": ";
    private static final String PARTITION_FIELD = "filename";
    private static final String OFFSET_FIELD = "position";

    private Long streamOffset = 0L;
    private String canonicalDirname;

    // private WatchService service;
    private ConnectDirectoryReader reader;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        LLNLDirectorySourceConfig config = new LLNLDirectorySourceConfig(map);
        try {
            String relativeDirname = config.getDirname();

            org.apache.avro.Schema avroSchema;
            if (!config.getAvroSchema().isEmpty()) {
                avroSchema = new org.apache.avro.Schema.Parser().parse(config.getAvroSchema());
            } else {
                avroSchema = new org.apache.avro.Schema.Parser().parse(new File(config.getAvroSchemaFilename()));
            }

            reader = new ConnectDirectoryReader(relativeDirname,
                                                config.getTopic(),
                                                avroSchema,
                                                config.getBatchSize(),
                                                PARTITION_FIELD,
                                                OFFSET_FIELD);

            canonicalDirname = reader.getCanonicalDirname();

            // log.info(TAG + "From directory {}, added all files: {}",
            //         canonicalDirname, String.join(":",reader.getFilenames()));

            // Path path = reader.getDirPath();
            // FileSystem fs = path.getFileSystem();

            // service = fs.newWatchService();
            // path.register(service, ENTRY_CREATE, ENTRY_DELETE);

        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        // TODO: directory watching does not work with NFS, check for new files inside of poll

        // Check directory for files added/removed
        // WatchKey key = service.poll();
        // if (key != null) {
        //     Path dirPath = (Path) key.watchable();
        //     // TODO: if dirPath is deleted, die!
        //     for (WatchEvent event : key.pollEvents()) {
        //         try {
        //             WatchEvent.Kind kind = event.kind();

        //             if (kind == ENTRY_CREATE) {
        //                 Path newPath = dirPath.resolve((Path) event.context());
        //                 File newFile = newPath.toFile();
        //                 reader.addFile(newFile);
        //                 log.info(TAG + "Added new file: {}", newFile.getName());
        //             } else if (kind == ENTRY_DELETE) {
        //                 Path deletedPath = dirPath.resolve((Path) event.context());
        //                 File deletedFile = deletedPath.toFile();
        //                 reader.removeFile(deletedFile);
        //                 log.info(TAG + "Removed deleted file: {}", deletedFile.getName());
        //             }

        //         } catch (IOException ex) {
        //             log.error(TAG, ex);
        //         }
        // }

        ArrayList<SourceRecord> records = new ArrayList<>();

        try {
            reader.read(records, context);
            return records;
        } catch (Exception e) {
            log.error(TAG, e);
        }
        return null;
    }

    @Override
    public void stop() {
        log.info(TAG + "stop");
        try {
            reader.close();
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }
}

