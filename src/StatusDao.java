import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.*;


public class StatusDao {

    private static boolean isNonEmptyString(String value) {
        return (value != null && value.length() > 0);
    }

    private static AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion("us-west-2")
            .build();
    private static DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

    public void addStatus(Status status) {
        Table table = dynamoDB.getTable("Status");

        Item item = new Item()
                .withPrimaryKey("username", status.getUsername(), "id", status.getId())
                .withString("name", status.getName())
                .withString("photo", status.getImage())
                .withString("status", status.getStatus())
                .withNumber("timestamp", status.getTimeStamp());

        if (status.getAttachment() != null) {
            item.withString("attachment", status.getAttachment());
            item.withString("type", status.getType());
        }
        table.putItem(item);


        Table hashTable = dynamoDB.getTable("Hashtag");
        if (status.getHashtags() != null) {
            for (int i = 0; i < status.getHashtags().size(); i++) {
                Item item1 = new Item()
                        .withPrimaryKey("hashtag", status.getHashtags().get(i), "id", status.getId())
                        .withString("username", status.getUsername())
                        .withString("name", status.getName())
                        .withString("photo", status.getImage())
                        .withString("status", status.getStatus())
                        .withNumber("timestamp", status.getTimeStamp());

                if (status.getAttachment() != null) {
                    item1.withString("attachment", status.getAttachment());
                    item1.withString("type", status.getType());
                }

                hashTable.putItem(item1);
            }
        }

//        List<User> users = new LinkedList<>();
//        FollowDao fd = new FollowDao();

//        UserResult results = null;
//        while (results == null || results.hasLastKey()) {
//            String lastUser = ((results != null) ? results.getLastKey() : null);
//            results = fd.getFollowerList(status.getUsername(), lastUser);
//            users.addAll(results.getValues());
//        }
//
//        Table table2 = dynamoDB.getTable("Feed");
//        for (User user : users) {
//            Item item2 = new Item()
//                    .withPrimaryKey("follower", user.getUsername(), "id", status.getId())
//                    .withString("name", status.getName())
//                    .withString("photo", status.getImage())
//                    .withString("status", status.getStatus())
//                    .withString("username", status.getUsername())
//                    .withNumber("timestamp", status.getTimeStamp());
//
//            if (status.getAttachment() != null) {
//                item2.withString("attachment", status.getAttachment());
//                item2.withString("type", status.getType());
//            }
//            table2.putItem(item2);
//        }
    }

    public boolean addToFeed(List<User> users, Status status) {
        Collection<Item> items = new ArrayList<>();
        for (User user : users) {
            Item item = new Item()
                    .withPrimaryKey("follower", user.getUsername(), "id", status.getId())
                    .withString("name", status.getName())
                    .withString("photo", status.getImage())
                    .withString("status", status.getStatus())
                    .withString("username", status.getUsername())
                    .withNumber("timestamp", status.getTimeStamp());

            if (status.getAttachment() != null) {
                item.withString("attachment", status.getAttachment());
                item.withString("type", status.getType());
            }
            items.add(item);
        }

        System.out.println(items.toArray());

        TableWriteItems witems = new TableWriteItems("Feed")
                .withItemsToPut(items);

        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(witems);
        System.out.println(outcome.toString());
        System.out.println(outcome.getBatchWriteItemResult().toString());

        do {
            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

            if (outcome.getUnprocessedItems().size() == 0) {
                System.out.println("No unprocessed items found");
            } else {
                System.out.println("Retrieving the unprocessed items");
                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
            }

        } while (outcome.getUnprocessedItems().size() > 0);


        return true;
    }

    public StatusResult userStatus(String username, String lastStatus) {
        StatusResult result = new StatusResult();

        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#username", "username");

        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":username", new AttributeValue().withS(username));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName("Status")
                .withKeyConditionExpression("#username = :username")
                .withExpressionAttributeNames(attrNames)
                .withExpressionAttributeValues(attrValues)
                .withLimit(3)
                .withScanIndexForward(false);

        if (isNonEmptyString(lastStatus)) {
            Map<String, AttributeValue> startKey = new HashMap<>();
            startKey.put("username", new AttributeValue().withS(username));
            startKey.put("timestamp", new AttributeValue().withN(lastStatus));

            queryRequest = queryRequest.withExclusiveStartKey(startKey);
        }

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                Status status = new Status();
                status.setUsername(item.get("username").getS());
                status.setName(item.get("name").getS());
                status.setImage(item.get("photo").getS());
                status.setStatus(item.get("status").getS());
                status.setTimeStamp(Long.parseLong(item.get("timestamp").getN()));
                status.setId(item.get("id").getS());

                if (item.get("attachment") != null) {
                    status.setAttachment(item.get("attachment").getS());
                    status.setType(item.get("type").getS());
                }
                result.addValue(status);
            }
        }

        Map<String, AttributeValue> lastKey = queryResult.getLastEvaluatedKey();
        if (lastKey != null) {
            result.setLastKey(lastKey.get("timestamp").getN());
        }
        else {
            result.setLastKey(null);
        }

        return result;
    }

    public StatusResult hashtags(String hashtag, String lastStatus) {
        StatusResult result = new StatusResult();

        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#hashtag", "hashtag");

        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":hashtag", new AttributeValue().withS(hashtag));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName("Hashtag")
                .withKeyConditionExpression("#hashtag = :hashtag")
                .withExpressionAttributeNames(attrNames)
                .withExpressionAttributeValues(attrValues)
                .withLimit(3)
                .withScanIndexForward(false);

        if (isNonEmptyString(lastStatus)) {
            Map<String, AttributeValue> startKey = new HashMap<>();
            startKey.put("hashtag", new AttributeValue().withS(hashtag));
            startKey.put("timestamp", new AttributeValue().withN(lastStatus));

            queryRequest = queryRequest.withExclusiveStartKey(startKey);
        }

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                Status status = new Status();
                status.setUsername(item.get("username").getS());
                status.setName(item.get("name").getS());
                status.setImage(item.get("photo").getS());
                status.setStatus(item.get("status").getS());
                status.setTimeStamp(Long.parseLong(item.get("timestamp").getN()));
                status.setId(item.get("id").getS());

                if (item.get("attachment") != null) {
                    status.setAttachment(item.get("attachment").getS());
                    status.setType(item.get("type").getS());
                }
                result.addValue(status);
            }
        }

        Map<String, AttributeValue> lastKey = queryResult.getLastEvaluatedKey();

        if (lastKey != null) {
            result.setLastKey(lastKey.get("timestamp").getN());
        }
        else {
            result.setLastKey(null);
        }

        return result;
    }

    public StatusResult userFeed(String username, String lastStatus) {
        StatusResult result = new StatusResult();

        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#follower", "follower");

        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":follower", new AttributeValue().withS(username));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName("Feed")
                .withKeyConditionExpression("#follower = :follower")
                .withExpressionAttributeNames(attrNames)
                .withExpressionAttributeValues(attrValues)
                .withLimit(3)
                .withScanIndexForward(false);


        if (isNonEmptyString(lastStatus)) {
            Map<String, AttributeValue> startKey = new HashMap<>();
            startKey.put("follower", new AttributeValue().withS(username));
            startKey.put("timestamp", new AttributeValue().withN(lastStatus));

            queryRequest = queryRequest.withExclusiveStartKey(startKey);
        }

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);

        List<Map<String, AttributeValue>> items = queryResult.getItems();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                Status status = new Status();
                status.setUsername(item.get("username").getS());
                status.setName(item.get("name").getS());
                status.setImage(item.get("photo").getS());
                status.setStatus(item.get("status").getS());
                status.setTimeStamp(Long.parseLong(item.get("timestamp").getN()));
                status.setId(item.get("id").getS());

                if (item.get("attachment") != null) {
                    status.setAttachment(item.get("attachment").getS());
                    status.setType(item.get("type").getS());
                }
                result.addValue(status);
            }
        }

        Map<String, AttributeValue> lastKey = queryResult.getLastEvaluatedKey();
        if (lastKey != null) {
            result.setLastKey(lastKey.get("timestamp").getN());
        }
        else {
            result.setLastKey(null);
        }

        return result;
    }
}
