package com.task05;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
    lambdaName = "api_handler",
	roleName = "api_handler-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
    @EnvironmentVariable(key = "region", value = "${region}"),
    @EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class ApiHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Gson gson = new Gson();
    private static final JsonParser jsonParser = new JsonParser();
    private static final String DYNAMODB_TABLE = System.getenv("target_table");

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        DynamoDbClient dynamoDbClient = DynamoDbClient.create();

        try {

            JsonObject requestBody = jsonParser.parse(input.getBody()).getAsJsonObject();
            int principalId = requestBody.get("principalId").getAsInt();
            JsonObject content = requestBody.get("content").getAsJsonObject();


            String eventId = UUID.randomUUID().toString();
            String createdAt = Instant.now().toString();

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s(eventId).build());
            item.put("principalId", AttributeValue.builder().n(String.valueOf(principalId)).build());
            item.put("createdAt", AttributeValue.builder().s(createdAt).build());
            item.put("body", AttributeValue.builder().s(content.toString()).build());


            PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(DYNAMODB_TABLE)
                .item(item)
                .build();

            PutItemResponse putItemResponse = dynamoDbClient.putItem(putItemRequest);


            JsonObject eventResponse = new JsonObject();
            eventResponse.addProperty("id", eventId);
            eventResponse.addProperty("principalId", principalId);
            eventResponse.addProperty("createdAt", createdAt);
            eventResponse.add("body", content);

            JsonObject responseBody = new JsonObject();
            responseBody.addProperty("statusCode", 201);
            responseBody.add("event", eventResponse);

            return new APIGatewayProxyResponseEvent()
                .withStatusCode(201)
                .withBody(gson.toJson(responseBody));

        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
            return new APIGatewayProxyResponseEvent()
                .withStatusCode(500)
                .withBody("{\"error\": \"" + e.getMessage() + "\"}");
        } finally {
            dynamoDbClient.close();
        }
    }
}
