using System.Text.Json;
using System.Text.Json.Serialization;

namespace api.Models.Converter
{
    public class ConfidenceConverter : JsonConverter<string>
    {

        public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.TokenType switch
            {
                JsonTokenType.Number => reader.GetInt32().ToString(),
                JsonTokenType.String => reader.GetString(),
                _ => null
            };
        }

        public override void Write(Utf8JsonWriter writer, string? value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }
}
