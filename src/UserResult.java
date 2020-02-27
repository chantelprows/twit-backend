import java.util.ArrayList;
import java.util.List;

public class UserResult {

    private List<User> values;

    private String lastKey;

    public UserResult() {
        values = new ArrayList<>();
        lastKey = null;
    }

    public void addValue(User v) {
        values.add(v);
    }

    public boolean hasValues() {
        return (values != null && values.size() > 0);
    }

    public List<User> getValues() {
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
