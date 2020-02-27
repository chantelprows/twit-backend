import com.amazonaws.partitions.model.Partition;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FollowEditor {

    private FollowDao fd = new FollowDao();
    private UserDao ud = new UserDao();
    private UserResult followingResult = new UserResult();
    private UserResult followerResult = new UserResult();

    public boolean getRelationship(Relationship relationship, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering getRelationship");

        return fd.getRelationship(relationship);
    }

    public Message follow(Relationship relationship, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering follow");

        fd.follow(relationship);

        Message message = new Message();
        message.setMessage("Follow Successful");

        return message;
    }

    public Message unfollow(Relationship relationship, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering follow");

       fd.unfollow(relationship);

        Message message = new Message();
        message.setMessage("Unfollow Successful");

        return message;
    }

    public PaginatedUsers getFollowList(FollowList followList, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering follow");

        if (!followingResult.hasLastKey() && followList.getPageNum() != 0) {
            PaginatedUsers users = new PaginatedUsers();
            users.setUserList(Collections.emptyList());
            users.setEnd(true);
            return users;
        }

        if (followList.getPageNum() == 0) {
            followingResult.setLastKey(null);
        }

        followingResult = fd.getFollowingList(followList.getUsername(), followingResult.getLastKey());

        PaginatedUsers users = new PaginatedUsers();
        users.setUserList(followingResult.getValues());
        if (!followingResult.hasLastKey()) {
            users.setEnd(true);
        }

        return users;
    }

    public PaginatedUsers getFollowerList(FollowList followList, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering follow");

        if (!followerResult.hasLastKey() && followList.getPageNum() != 0) {
            PaginatedUsers users = new PaginatedUsers();
            users.setUserList(Collections.emptyList());
            users.setEnd(true);
            return users;
        }

        if (followList.getPageNum() == 0) {
            followerResult.setLastKey(null);
        }

        followerResult = fd.getFollowerList(followList.getUsername(), followerResult.getLastKey());

        PaginatedUsers users = new PaginatedUsers();
        users.setUserList(followerResult.getValues());
        if (!followerResult.hasLastKey()) {
            users.setEnd(true);
        }

        return users;

    }

    public static <T> Stream<List<T>> batches(List<T> source, int length) {
        if (length <= 0)
            throw new IllegalArgumentException("length = " + length);
        int size = source.size();
        if (size <= 0)
            return Stream.empty();
        int fullChunks = (size - 1) / length;
        return IntStream.range(0, fullChunks + 1).mapToObj(
                n -> source.subList(n * length, n == fullChunks ? size : (n + 1) * length));
    }

    public void readFollowers(SQSEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("TRY 2: ");

        logger.log(event.toString());
        Status status = null;
        Gson g = new Gson();
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            logger.log(message.getBody());
            status = g.fromJson(message.getBody(), Status.class);
            logger.log("STATUS: " + status + " AND USRENAME: " + status.getUsername());
        }

        UserResult ur = fd.getEntireFollowerList(status.getUsername());
        logger.log("LIST: " + ur.getValues().size());
        List<User> allValues = ur.getValues();

        for (int i = 0; i < allValues.size() / 25; i++) {
            UsersAndStatus us = new UsersAndStatus();
            us.setUsers(allValues.subList(i * 25, i * 25 + 25));
            us.setStatus(status);
            Gson gson = new Gson();
            String json = gson.toJson(us);
            logger.log("JSON: " + json);

            String qURL = "https://sqs.us-west-2.amazonaws.com/043760353468/writeStatuses";
            SendMessageRequest request = new SendMessageRequest()
                    .withQueueUrl(qURL)
                    .withMessageBody(json);

            AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
            SendMessageResult result = sqs.sendMessage(request);
        }

        logger.log("MADE IT TO END");
    }

//    public boolean followABunch() {
//
//        for (int i = 0; i < 10000; i++) {
//            User user = new User();
//            user.setPhoto("https://picsum.photos/200");
//            user.setName(Integer.toString(i));
//            user.setUsername(Integer.toString(i));
//            ud.addUser(user);
//        }
//
//        for (int i = 0; i < 10000; i++) {
//            Relationship relationship = new Relationship();
//            relationship.setFollowee("chantelprows");
//            relationship.setFolloweeName("Chantel Prows");
//            relationship.setFolloweePhoto("https://photos-cs340.s3-us-west-2.amazonaws.com/photo/chantelprows");
//            relationship.setFollower(Integer.toString(i));
//            relationship.setFollowerName(Integer.toString(i));
//            relationship.setFollowerPhoto("https://picsum.photos/200");
//            fd.follow(relationship);
//        }
//
//        return true;
//
//    }

    public static void main(String[] args) {
        FollowEditor fe = new FollowEditor();
        FollowDao fd = new FollowDao();
        //fe.followABunch();
        UserResult result = fd.getEntireFollowerList("chantelprows");
        System.out.println(Integer.toString(result.getValues().size()));
    }
}
