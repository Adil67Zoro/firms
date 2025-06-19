using CsvHelper;
using CsvHelper.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Globalization;
using System.Text.Json;
using System.Net.Http;
using System.Collections.Concurrent;

class Program
{
    static readonly string MAP_KEY = "7db0ce61ad6b61b0f85ddd76641e1df6";
    static readonly string MONGO_URI = "mongodb://admin:StrongPassword123@mongodb:27017/?authSource=admin";
    static readonly string DB_NAME = "fires";
    static readonly string COLLECTION_NAME = "Fires";
    static readonly int Days = 2;
    static readonly TimeZoneInfo KZ_TZ = TimeZoneInfo.FindSystemTimeZoneById("Asia/Qyzylorda");
    static readonly HttpClient client = new();

    static readonly List<string> Urls = new()
    {
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/MODIS_NRT/KAZ/{Days}",
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/KAZ/{Days}",
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_NRT/KAZ/{Days}",
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_SP/KAZ/{Days}"
    };

    static async Task Main(string[] args)
    {
        while (true)
        {
            await FetchAndUpload();
            Thread.Sleep(TimeSpan.FromMinutes(10));
        }
    }

    static async Task FetchAndUpload()
    {
        var allRecords = new List<Dictionary<string, string>>();

        var contentToken = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            { "username", "adiltest" },
            { "password", "y!6iyT59B9k.Hth" },
            { "referer", "https://www.arcgis.com" },
            { "f", "json"}
        });

        var responseToken = await client.PostAsync("https://www.arcgis.com/sharing/rest/generateToken", contentToken);
        var token = JsonDocument.Parse(await responseToken.Content.ReadAsStringAsync())
            .RootElement.GetProperty("token").GetString()!;

        var url1 = "https://services3.arcgis.com/RGg2rzCtnLDgGhvB/arcgis/rest/services/KZ_Regions/FeatureServer/1/query";
        var url2 = "https://services3.arcgis.com/RGg2rzCtnLDgGhvB/arcgis/rest/services/KZ_Regions/FeatureServer/2/query";

        foreach (var url in Urls)
        {
            try
            {
                var response = await client.GetStringAsync(url);
                using var reader = new StringReader(response);
                using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
                var records = csv.GetRecords<dynamic>().ToList();

                foreach (var r in records)
                {
                    var dict = new Dictionary<string, string>();
                    foreach (var kv in r) dict[kv.Key] = kv.Value?.ToString() ?? "";
                    allRecords.Add(dict);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to read from {url}: {ex.Message}");
            }
        }

        if (!allRecords.Any())
        {
            Console.WriteLine("No data fetched from any source. Skipping MongoDB update.");
            return;
        }

        var mongoClient = new MongoClient(MONGO_URI);
        var collection = mongoClient.GetDatabase(DB_NAME).GetCollection<BsonDocument>(COLLECTION_NAME);
        var existing = collection.Find(FilterDefinition<BsonDocument>.Empty)
            .Project(Builders<BsonDocument>.Projection.Include("latitude").Include("longitude").Include("sputnik_recorded_datetime"))
            .ToList()
            .Select(doc => (doc["latitude"].ToDouble(), doc["longitude"].ToDouble(), doc["sputnik_recorded_datetime"]))
            .ToHashSet();

        var now = TimeZoneInfo.ConvertTime(DateTime.Now, KZ_TZ);
        var apiRequestedStr = now.ToString("yyyy-MM-ddTHH:mm:ss+0500");
        var typedRecords = new ConcurrentBag<BsonDocument>();

        await Parallel.ForEachAsync(allRecords, async (record, ct) =>
        {
            try
            {
                record["api_requested_datetime"] = apiRequestedStr;
                var acqDate = record.GetValueOrDefault("acq_date");
                var acqTime = record.GetValueOrDefault("acq_time").PadLeft(4, '0');
                var dtStr = $"{acqDate} {acqTime[..2]}:{acqTime[2..]}";
                var dt = DateTime.ParseExact(dtStr, "yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture).AddHours(5);
                record["sputnik_recorded_datetime"] = dt.ToString("yyyy-MM-ddTHH:mm:ss+0500");
                record.Remove("acq_date");
                record.Remove("acq_time");

                var lat = double.Parse(record["latitude"], CultureInfo.InvariantCulture);
                var lon = double.Parse(record["longitude"], CultureInfo.InvariantCulture);
                var dtKey = record["sputnik_recorded_datetime"];

                if (existing.Contains((lat, lon, dtKey))) return;

                if (record.TryGetValue("instrument", out var instr))
                {
                    if (instr == "MODIS") { record.Remove("bright_ti4"); record.Remove("bright_ti5"); }
                    else if (instr == "VIIRS") { record.Remove("brightness"); record.Remove("bright_t31"); }
                }
                record.Remove("type");

                var typed = new BsonDocument();
                foreach (var kv in record)
                {
                    var key = kv.Key;
                    var value = kv.Value;
                    if (key.Contains("datetime")) typed[key] = value;
                    else if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double dbl))
                    {
                        if (value.Contains('.')) typed[key] = dbl;
                        else if (int.TryParse(value, out int i)) typed[key] = i;
                        else typed[key] = dbl;
                    }
                    else typed[key] = value;
                }

                string geometry = $"{record["longitude"]}, {record["latitude"]}";

                var admin1 = await client.PostAsync(url1, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"f", "json"},
                    {"geometry", geometry},
                    {"geometryType", "esriGeometryPoint"},
                    {"inSR", "4326"},
                    {"spatialRel", "esriSpatialRelIntersects"},
                    {"outFields", "NAME_1"},
                    {"returnGeometry", "false"},
                    {"token", token}
                }));

                var admin2 = await client.PostAsync(url2, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"f", "json"},
                    {"geometry", geometry},
                    {"geometryType", "esriGeometryPoint"},
                    {"inSR", "4326"},
                    {"spatialRel", "esriSpatialRelIntersects"},
                    {"outFields", "NAME_2"},
                    {"returnGeometry", "false"},
                    {"token", token}
                }));

                using var doc1 = JsonDocument.Parse(await admin1.Content.ReadAsStringAsync());
                using var doc2 = JsonDocument.Parse(await admin2.Content.ReadAsStringAsync());
                var features1 = doc1.RootElement.GetProperty("features");
                var features2 = doc2.RootElement.GetProperty("features");
                if (features1.GetArrayLength() == 0 || features2.GetArrayLength() == 0) return;

                typed["oblast"] = features1[0].GetProperty("attributes").GetProperty("NAME_1").GetString();
                typed["raion"] = features2[0].GetProperty("attributes").GetProperty("NAME_2").GetString();

                typedRecords.Add(typed);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Skipping record due to error: {ex.Message}");
            }
        });

        if (typedRecords.Any())
        {
            await collection.InsertManyAsync(typedRecords);
            Console.WriteLine($"Inserted {typedRecords.Count} documents at {apiRequestedStr}");
        }
        else
        {
            Console.WriteLine("No records to insert. Skipping MongoDB update.");
        }
    }
}
