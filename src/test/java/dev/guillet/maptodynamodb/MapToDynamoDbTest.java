package dev.guillet.maptodynamodb;

import static dev.guillet.maptodynamodb.MapToDynamoDb.mapToDynamoDbAttributeValues;
import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider.defaultProvider;
import static software.amazon.awssdk.enhanced.dynamodb.TableMetadata.primaryIndexName;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.Map;

@Testcontainers
public class MapToDynamoDbTest {

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5.0"))
            .withServices(LocalStackContainer.Service.DYNAMODB);

    @Test
    public void shouldInsertMapIntoDynamoDbTable() {

        //given
        var dynamoDbEnhancedClient = DynamoDbEnhancedClient
                .builder()
                .dynamoDbClient(DynamoDbClient.builder()
                        .endpointOverride(localstack.getEndpoint())
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                                )
                        )
                        .region(Region.of(localstack.getRegion()))
                        .build())
                .build();

        var dynamoDbTable = dynamoDbEnhancedClient
                .table("tableName",
                        TableSchema.documentSchemaBuilder()
                                .addIndexPartitionKey(primaryIndexName(), "id", AttributeValueType.S)
                                .attributeConverterProviders(defaultProvider())
                                .build());

        var map = Map.of(
                "id", "1",
                "attributeString", "value1",
                "attributeInteger", 1,
                "attributeDouble", 2.0,
                "attributeLong", 3L,
                "attributeBoolean", true
        );

        //when
        dynamoDbTable.createTable();
        dynamoDbTable.putItem(EnhancedDocument.builder()
                .attributeValueMap(mapToDynamoDbAttributeValues(map))
                .build());

        //then
        var item = dynamoDbTable.getItem(Key.builder().partitionValue("1").build());
        assertNotNull(item);
        assertEquals("value1", item.getString("attributeString"));
        assertEquals(1, item.getNumber("attributeInteger").intValue());
        assertEquals(2.0, item.getNumber("attributeDouble").doubleValue());
        assertEquals(3L, item.getNumber("attributeLong").longValue());
        assertTrue(item.getBoolean("attributeBoolean"));

    }

}
