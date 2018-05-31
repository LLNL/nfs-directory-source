package gov.llnl.sonar.kafka.connect.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class BackupUtil {
    static public void createBackupTar(Path source, Path destination) {
        try {
            String compressedFilename = Paths.get(destination.toString(), source.getFileName() + ".bak.tar.gz").toString();
            OutputStream fo = new FileOutputStream(compressedFilename);
            OutputStream gzo = new GzipCompressorOutputStream(fo);

            TarArchiveOutputStream out = new TarArchiveOutputStream(gzo);

            Files.walk(source).filter(Files::isRegularFile).forEach(path -> {
                try {
                    // Create archive entry with file
                    path.relativize(source);
                    out.putArchiveEntry(new TarArchiveEntry(path.toFile()));

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
            out.close();
        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException:", e);
        } catch (IOException e) {
            log.error("IOException:", e);
        }

    }
}
