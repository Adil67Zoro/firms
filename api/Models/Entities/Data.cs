using api.Serializer;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Text.Json.Serialization;

namespace api.Models.Entities
{
    public class Data
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string? Id { get; set; }

        [BsonElement("country_id")]
        public string Country_Id { get; set; } = null!;

        [BsonElement("latitude")]
        public double Latitude { get; set; }

        [BsonElement("longitude")]
        public double Longitude { get; set; }

        [BsonElement("brightness")]
        [BsonIgnoreIfNull]
        public double? Brightness { get; set; }
        public bool ShouldSerializeBrightness()
        {
            return Instrument == "MODIS";
        }

        [BsonElement("scan")]
        public double Scan { get; set; }

        [BsonElement("track")]
        public double Track { get; set; }

        [BsonElement("satellite")]
        public string Satellite { get; set; } = null!;

        [BsonElement("instrument")]
        public string Instrument { get; set; } = null!;

        [BsonElement("confidence")]
        [BsonSerializer(typeof(ConfidenceBsonSerializer))]
        public string? Confidence { get; set; }

        [BsonElement("version")]
        public string Version { get; set; } = null!;

        [BsonElement("bright_t31")]
        [BsonIgnoreIfNull]
        public double? Bright_t31 { get; set; }
        public bool ShouldSerializeBright_t31()
        {
            return Instrument == "MODIS";
        }

        [BsonElement("frp")]
        public double Frp { get; set; }

        [BsonElement("daynight")]
        public string Daynight { get; set; } = null!;

        [BsonElement("bright_ti4")]
        [BsonIgnoreIfNull]
        public double? Bright_ti4 { get; set; }

        public bool ShouldSerializeBright_ti4()
        {
            return Instrument == "VIIRS";
        }

        [BsonElement("bright_ti5")]
        [BsonIgnoreIfNull]
        public double? Bright_ti5 { get; set; }

        public bool ShouldSerializeBright_ti5()
        {
            return Instrument == "VIIRS";
        }

        [BsonElement("api_requested_datetime")]
        public string Api_Requested_DateTime { get; set; } = null!;

        [BsonElement("sputnik_recorded_datetime")]
        public string Sputnik_Recorded_DateTime { get; set; } = null!;
    }
}
