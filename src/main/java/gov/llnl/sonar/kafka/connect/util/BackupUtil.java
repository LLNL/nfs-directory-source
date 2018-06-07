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
                    File file = path.toFile();
                    out.putArchiveEntry(new TarArchiveEntry(file, source.relativize(path).toString()));

                    // Write archive entry with file contents
                    InputStream i = new FileInputStream(file);
                    IOUtils.copy(i, out);
                    i.close();

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
