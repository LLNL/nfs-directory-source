package gov.llnl.sonar.kafka.connect.readers;

import java.io.Serializable;

public class FileOffset implements Serializable {
    Long offset;
    boolean locked;
    boolean completed;

    private static final long serialVersionUID = 1L;

    public FileOffset(Long offset, boolean locked, boolean completed) {
        this.offset = offset;
        this.locked = locked;
        this.completed = completed;
    }

    public void setLocked(boolean l) {
        locked = l;
    }

    public void setCompleted(boolean c) {
        completed = c;
    }

    public void setOffset(Long l) {
        offset = l;
    }

    @Override
    public String toString() {
        return String.format("FileOffset(offset=%d,locked=%b,completed=%b)", offset, locked, completed);
    }
}

