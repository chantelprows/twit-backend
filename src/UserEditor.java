import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.util.LinkedList;
import java.util.List;

public class UserEditor {

    private List<User> userList = new LinkedList<>();
    private UserDao ud = new UserDao();

    public User getUser(User user, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering getUser");

        User retUser = ud.getUser(user.getUsername());

        return retUser;
    }

    public User addUser(User user, Context context) {
        User retUser = ud.addUser(user);
        return retUser;
    }

    public Message changePhoto(Relationship photo, Context context) {
        ud.changePhoto(photo.getFollowee(), photo.getFollower()); //photo, username
        Message mes = new Message();
        mes.setMessage("Change successful");
        return mes;
    }
}
