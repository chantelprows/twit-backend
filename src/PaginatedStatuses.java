import java.util.LinkedList;
import java.util.List;

public class PaginatedStatuses {

    private List<Status> statuses = new LinkedList<>();
    private boolean isEnd = false;

    public List<Status> getStatuses() {
        return statuses;
    }

    public void setStatuses(List<Status> statuses) {
        this.statuses = statuses;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

}
