import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FollowDao {

    private static boolean isNonEmptyString(String value) {
        return (value != null && value.length() > 0);
    }

    private static AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion("us-west-2")
            .build();
    private static DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

    public Boolean getRelationship(Relationship relationship) {
        Table table = dynamoDB.getTable("Follow");

        Item item = table.getItem("follower", relationship.getFollower(),
                "followee", relationship.getFollowee());

        if (item == null) {
            return false;
        }

        return true;
    }

    public void follow(Relationship relationship) {
        Table table = dynamoDB.getTable("Follow");
        Table table2 = dynamoDB.getTable("Followers");

        Item item = new Item()
                .withPrimaryKey("follower", relationship.getFollower(),
                        "followee", relationship.getFollowee())
                .withString("followeeName", relationship.getFolloweeName())
                .withString("followeePhoto", relationship.getFolloweePhoto());

        table.putItem(item);

        Item item2 = new Item()
                .withPrimaryKey("followee", relationship.getFollowee(),
                        "follower", relationship.getFollower())
                .withString("followerName", relationship.getFollowerName())
                .withString("followerPhoto", relationship.getFollowerPhoto());

        table2.putItem(item2);
    }

    public void unfollow(Relationship relationship) {
        Table table = dynamoDB.getTable("Follow");

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey("follower", relationship.getFollower(),
                        "followee", relationship.getFollowee());

        table.deleteItem(deleteItemSpec);

        Table table2 = dynamoDB.getTable("Followers");

        DeleteItemSpec deleteItemSpec2 = new DeleteItemSpec()
                .withPrimaryKey("followee", relationship.getFollowee(),
                        "follower", relationship.getFollower());

        table2.deleteItem(deleteItemSpec2);

    }

    public UserResult getFollowingList(String username, String lastFollowee) {
        UserResult result = new UserResult();

        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#follower", "follower");

        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":username", new AttributeValue().withS(username));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName("Follow")
                .withKeyConditionExpression("#follower = :username")
                .withExpressionAttributeNames(attrNames)
                .withExpressionAttributeValues(attrValues)
                .withLimit(1);

        if (isNonEmptyString(lastFollowee)) {
            Map<String, AttributeValue> startKey = new HashMap<>();
            startKey.put("follower", new AttributeValue().withS(username));
            startKey.put("followee", new AttributeValue().withS(lastFollowee));

            queryRequest = queryRequest.withExclusiveStartKey(startKey);
        }

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                User user = new User();
                user.setUsername(item.get("followee").getS());
                user.setName(item.get("followeeName").getS());
                user.setPhoto(item.get("followeePhoto").getS());
                result.addValue(user);
            }
        }

        Map<String, AttributeValue> lastKey = queryResult.getLastEvaluatedKey();
        if (lastKey != null) {
            result.setLastKey(lastKey.get("followee").getS());
        }
        else {
            result.setLastKey(null);
        }

        return result;
    }

    public UserResult getFollowerList(String username, String lastFollower) {
        UserResult result = new UserResult();

        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#followee", "followee");

        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":username", new AttributeValue().withS(username));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName("Followers")
                .withKeyConditionExpression("#followee = :username")
                .withExpressionAttributeNames(attrNames)
                .withExpressionAttributeValues(attrValues)
                .withLimit(1);

        if (isNonEmptyString(lastFollower)) {
            Map<String, AttributeValue> startKey = new HashMap<>();
            startKey.put("followee", new AttributeValue().withS(username));
            startKey.put("follower", new AttributeValue().withS(lastFollower));

            queryRequest = queryRequest.withExclusiveStartKey(startKey);
        }

        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                User user = new User();
                user.setUsername(item.get("follower").getS());
                user.setName(item.get("followerName").getS());
                user.setPhoto(item.get("followerPhoto").getS());
                result.addValue(user);
            }
        }

        Map<String, AttributeValue> lastKey = queryResult.getLastEvaluatedKey();
        if (lastKey != null) {
            result.setLastKey(lastKey.get("follower").getS());
        }
        else {
            result.setLastKey(null);
        }

        return result;
    }

    public UserResult getEntireFollowerList(String username) {
        UserResult result = new UserResult();

        Map<String, String> attrNames = new HashMap<>();
        attrNames.put("#followee", "followee");

        Map<String, AttributeValue> attrValues = new HashMap<>();
        attrValues.put(":username", new AttributeValue().withS(username));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName("Followers")
                .withKeyConditionExpression("#followee = :username")
                .withExpressionAttributeNames(attrNames)
                .withExpressionAttributeValues(attrValues);


        QueryResult queryResult = amazonDynamoDB.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResult.getItems();
        if (items != null) {
            for (Map<String, AttributeValue> item : items) {
                User user = new User();
                user.setUsername(item.get("follower").getS());
                user.setName(item.get("followerName").getS());
                user.setPhoto(item.get("followerPhoto").getS());
                result.addValue(user);
            }
        }

        return result;
    }
}
