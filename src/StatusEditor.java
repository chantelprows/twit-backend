import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.*;

public class StatusEditor {

    private StatusDao sd = new StatusDao();
    private StatusResult story = new StatusResult();
    private StatusResult hashtags = new StatusResult();
    private StatusResult feed = new StatusResult();
    private String qURL = "https://sqs.us-west-2.amazonaws.com/043760353468/readFollowers";

    public static void main(String[] args) {
        StatusEditor se = new StatusEditor();
        Status status = new Status();
        status.setName("nothing");
        status.setUsername("nothing");
        status.setImage("hi");
        status.setId("15");
        status.setTimeStamp(1574298294039L);
        status.setStatus("hi");
//        se.addStatus(status);
    }

    public Message addStatus(Status status, Context context) {
        LambdaLogger logger = context.getLogger();
        sd.addStatus(status);

        Gson gson = new Gson();
        String json = gson.toJson(status);

        SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(qURL)
                .withMessageBody(json);

        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        SendMessageResult result = sqs.sendMessage(request);

        Message mes = new Message();
        mes.setMessage("Add successful");
        return mes;
    }

    public PaginatedStatuses userStatus(FollowList user, Context context) {

        if (!story.hasLastKey() && user.getPageNum() != 0) {
            PaginatedStatuses statuses = new PaginatedStatuses();
            statuses.setStatuses(Collections.emptyList());
            statuses.setEnd(true);
            return statuses;
        }

        if (user.getPageNum() == 0) {
            story.setLastKey(null);
        }

        story = sd.userStatus(user.getUsername(), story.getLastKey());

        PaginatedStatuses statuses = new PaginatedStatuses();
        statuses.setStatuses(story.getValues());
        if (!story.hasLastKey()) {
            statuses.setEnd(true);
        }

        return statuses;
    }

    public PaginatedStatuses userFeed(FollowList user, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Entering feed");

        if (!feed.hasLastKey() && user.getPageNum() != 0) {
            PaginatedStatuses statuses = new PaginatedStatuses();
            statuses.setStatuses(Collections.emptyList());
            statuses.setEnd(true);
            return statuses;
        }

        if (user.getPageNum() == 0) {
            feed.setLastKey(null);
        }

        feed = sd.userFeed(user.getUsername(), feed.getLastKey());

        PaginatedStatuses statuses = new PaginatedStatuses();
        statuses.setStatuses(feed.getValues());
        if (!feed.hasLastKey()) {
            statuses.setEnd(true);
        }

        return statuses;
    }

    public PaginatedStatuses hashtags(FollowList hashtag, Context context) {
        LambdaLogger logger = context.getLogger();

        if (!hashtags.hasLastKey() && hashtag.getPageNum() != 0) {
            PaginatedStatuses statuses = new PaginatedStatuses();
            statuses.setStatuses(Collections.emptyList());
            statuses.setEnd(true);
            return statuses;
        }

        if (hashtag.getPageNum() == 0) {
            hashtags.setLastKey(null);
        }

        hashtags = sd.hashtags(hashtag.getUsername(), hashtags.getLastKey());

        PaginatedStatuses statuses = new PaginatedStatuses();
        statuses.setStatuses(hashtags.getValues());
        if (!hashtags.hasLastKey()) {
            statuses.setEnd(true);
        }

        return statuses;
    }

    public boolean writeToFeed(SQSEvent event, Context context) {
        LambdaLogger logger = context.getLogger();

        logger.log("TRY 2 EVENT: ");
        logger.log(event.toString());

        Status status = new Status();
        List<User> users = new ArrayList<>();
        UsersAndStatus us = new UsersAndStatus();
        Type type = new TypeToken<ArrayList<User>>(){}.getType();

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            logger.log(message.getBody());
            String json = message.getBody();
            Gson g = new Gson();
            us = g.fromJson(json, UsersAndStatus.class);
            status = us.getStatus();
            users = us.getUsers();
        }

        logger.log("USERS: " + users.size());
        logger.log("STATUS: " + status.getStatus());

        sd.addToFeed(users, status);
        
        return true;
    }
}
