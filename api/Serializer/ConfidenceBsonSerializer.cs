using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace api.Serializer;

public class ConfidenceBsonSerializer : SerializerBase<string>
{
    public override string Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var bsonType = context.Reader.GetCurrentBsonType();
        return bsonType switch
        {
            BsonType.Int32 => context.Reader.ReadInt32().ToString(),
            BsonType.String => context.Reader.ReadString(),
            _ => throw new FormatException($"Unsupported BsonType {bsonType} for Confidence field.")
        };
    }

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, string value)
    {
        context.Writer.WriteString(value);
    }
}

