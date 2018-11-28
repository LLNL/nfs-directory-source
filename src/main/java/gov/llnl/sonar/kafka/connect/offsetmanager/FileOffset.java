package gov.llnl.sonar.kafka.connect.offsetmanager;

import java.io.Serializable;

/**
 * Represents the current read state of a file.
 * Instances of FileOffset are meant to be serialized into Zookeeper nodes (via FileOffsetManager) for concurrent access.
 */
public class FileOffset implements Serializable {

    /**
     * Current read offset, in bytes.
     */
    Long offset;

    /**
     * Whether the file is currently locked for reading.
     */
    boolean locked;

    /**
     * Whether the file has been fully read.
     */
    boolean completed;

    private static final long serialVersionUID = 1L;

    public FileOffset() {
    }

    public FileOffset(Long offset, boolean locked, boolean completed) {
        this.offset = offset;
        this.locked = locked;
        this.completed = completed;
    }

    @Override
    public String toString() {
        return String.format("FileOffset(offset=%d,locked=%b,completed=%b)", offset, locked, completed);
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public Long getOffset() {
        return offset;
    }
}

