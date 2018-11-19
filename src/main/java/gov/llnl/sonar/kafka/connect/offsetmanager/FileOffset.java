package gov.llnl.sonar.kafka.connect.offsetmanager;

import java.io.Serializable;

public class FileOffset implements Serializable {
    Long offset;
    boolean locked;
    boolean completed;

    private static final long serialVersionUID = 1L;

    FileOffset(Long offset, boolean locked, boolean completed) {
        this.offset = offset;
        this.locked = locked;
        this.completed = completed;
    }

    @Override
    public String toString() {
        return String.format("FileOffset(getByteOffset=%d,locked=%b,completed=%b)", offset, locked, completed);
    }
}

