package gov.llnl.sonar.kafka.connect.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class BackupUtil {
    static public void createBackupTar(Path source, Path destination) {
        try {
            OutputStream fo = new FileOutputStream(destination.toString() + ".bak.tar.gz");
            OutputStream gzo = new GzipCompressorOutputStream(fo);

            TarArchiveOutputStream out = new TarArchiveOutputStream(gzo);

            Files.walk(source).filter(Files::isRegularFile).forEach(path -> {
                try {
                    // Create archive entry with relative path
                    Path relativePath = path.relativize(source);
                    ArchiveEntry archiveEntry = out.createArchiveEntry(path.toFile(), relativePath.toString());
                    out.putArchiveEntry(archiveEntry);

                    // Write archive entry with file contents
                    InputStream i = Files.newInputStream(path);
                    IOUtils.copy(i, out);

                    // Close archive entry
                    out.closeArchiveEntry();
                } catch (IOException e) {
                    log.error("IOException:", e);
                }
            });

            out.finish();
        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException:", e);
        } catch (IOException e) {
            log.error("IOException:", e);
        }

    }
}
