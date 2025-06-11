using System.Text.Json.Serialization;

namespace api.Models.DTOs
{
    public class DataDto
    {
        public string Country_Id { get; set; } = null!;

        public double Latitude { get; set; }

        public double Longitude { get; set; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public double? Brightness { get; set; }
        public double Scan { get; set; }
        public double Track { get; set; }
        public string Satellite { get; set; } = null!;
        public string Instrument { get; set; } = null!;
        public string? Confidence { get; set; }
        public string Version { get; set; } = null!;

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public double? Bright_t31 { get; set; }
        public double Frp { get; set; }
        public string Daynight { get; set; } = null!;

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public double? Bright_ti4 { get; set; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public double? Bright_ti5 { get; set; }
        public string Api_Requested_DateTime { get; set; } = null!;
        public string Sputnik_Recorded_DateTime { get; set; } = null!;
    }
}
