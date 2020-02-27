import java.util.ArrayList;
import java.util.List;

public class StatusResult {
    private List<Status> values;

    private String lastKey;

    public StatusResult() {
        values = new ArrayList<>();
        lastKey = null;
    }

    public void addValue(Status v) {
        values.add(v);
    }

    public boolean hasValues() {
        return (values != null && values.size() > 0);
    }

    public List<Status> getValues() {
        return values;
    }

    public void setLastKey(String value) {
        lastKey = value;
    }

    public String getLastKey() {
        return lastKey;
    }

    public boolean hasLastKey() {
        return (lastKey != null && lastKey.length() > 0);
    }
}
