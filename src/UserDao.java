import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;

public class UserDao {

    private static AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion("us-west-2")
            .build();
    private static DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

    public User addUser(User user) {
        Table table = dynamoDB.getTable("User");

        Item item = new Item()
                .withPrimaryKey("username", user.getUsername())
                .withString("name", user.getName())
                .withString("photo", user.getPhoto());
        table.putItem(item);

        return user;
    }

    public User getUser(String username) {
        Table table = dynamoDB.getTable("User");
        Item item = table.getItem("username", username);

        User user = new User();
        user.setUsername(item.getString("username"));
        user.setName(item.getString("name"));
        user.setPhoto(item.getString("photo"));

        return user;
    }

    public void changePhoto(String photo, String username) {

        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName("User")
                .addKeyEntry("username", new AttributeValue().withS(username))
                .addAttributeUpdatesEntry("photo", new AttributeValueUpdate().withValue(new AttributeValue().withS(photo)));

        amazonDynamoDB.updateItem(updateItemRequest);

    }

}
