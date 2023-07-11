package benchmark;

import java.sql.Timestamp;

// TODO: not sure about this class - WIP

public class MetricsCollector {
    private Timestamp pipeEntryTimestamp;
    private Timestamp pipeExitTimestamp;
    private Timestamp anonEntryTimestamp;
    private Timestamp anonExitTimestamp;

    public Timestamp getPipeEntryTimestamp() {
        return pipeEntryTimestamp;
    }

    public Timestamp getPipeExitTimestamp() {
        return pipeExitTimestamp;
    }

    public Timestamp getAnonEntryTimestamp() {
        return anonEntryTimestamp;
    }

    public Timestamp getAnonExitTimestamp() {
        return anonExitTimestamp;
    }

    public void setAnonEntryTimestamp(Timestamp anonEntryTimestamp) {
        this.anonEntryTimestamp = anonEntryTimestamp;
    }

    public void setPipeEntryTimestamp(Timestamp pipeEntryTimestamp) {
        this.pipeEntryTimestamp = pipeEntryTimestamp;
    }

    public void setPipeExitTimestamp(Timestamp pipeExitTimestamp) {
        this.pipeExitTimestamp = pipeExitTimestamp;
    }

    public void setAnonExitTimestamp(Timestamp anonExitTimestamp) {
        this.anonExitTimestamp = anonExitTimestamp;
    }
}
