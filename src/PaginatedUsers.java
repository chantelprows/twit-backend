import java.util.LinkedList;
import java.util.List;

public class PaginatedUsers {

    private List<User> userList = new LinkedList<>();
    private boolean isEnd = false;

    public List<User> getUserList() {
        return userList;
    }

    public void setUserList(List<User> userList) {
        this.userList = userList;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }
}
