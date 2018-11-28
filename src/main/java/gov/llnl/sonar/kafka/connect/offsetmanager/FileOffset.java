package gov.llnl.sonar.kafka.connect.offsetmanager;

import java.io.Serializable;

public class FileOffset implements Serializable {
    Long offset;
    boolean locked;
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

