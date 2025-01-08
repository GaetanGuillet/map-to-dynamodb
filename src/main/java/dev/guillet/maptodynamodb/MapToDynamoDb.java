package dev.guillet.maptodynamodb;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.Pair;

import java.util.Map;
import java.util.stream.Collectors;

public class MapToDynamoDb {

    public static Map<String, AttributeValue> mapToDynamoDbAttributeValues(Map<String, ?> map) {
        return map.entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), mapToDynamoDbAttributeValue(entry.getValue())))
                .collect(Collectors.toMap(Pair::left, Pair::right));
    }

    private static AttributeValue mapToDynamoDbAttributeValue(Object object) {
        return switch (object) {
            case null -> AttributeValue.fromNul(true);
            case Boolean b -> AttributeValue.fromBool(b);
            case Number n -> AttributeValue.fromN(n.toString());
            case String s -> AttributeValue.fromS(s);
            default -> throw new IllegalStateException("Unexpected value: " + object.getClass().getSimpleName());
        };
    }

}
