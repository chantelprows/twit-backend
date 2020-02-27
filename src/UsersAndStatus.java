import java.util.ArrayList;
import java.util.List;

public class UsersAndStatus {

    private List<User> users = new ArrayList();
    private Status status = new Status();

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
