using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;
using System.Globalization;

namespace fires.Services
{
    public class DataParser
    {
        private readonly IMongoCollection<BsonDocument> _OblastCollection;
        private readonly IMongoCollection<BsonDocument> _RaionCollection;
        private readonly IMongoCollection<BsonDocument> _NotFireCollection;
        private readonly Helper _helper;
        private readonly MongodbInserter _mongodbInserter;
        private readonly ImageAdder _imageAdder;

        public DataParser(IConfiguration config, Helper helper, MongodbInserter mongodbInserter, ImageAdder imageAdder)
        {
            var client = new MongoClient(config.GetConnectionString("Default"));
            var database = client.GetDatabase("fires");
            _OblastCollection = database.GetCollection<BsonDocument>("Oblast");
            _RaionCollection = database.GetCollection<BsonDocument>("Raion");
            _NotFireCollection = database.GetCollection<BsonDocument>("NotFireData");
            _helper = helper;
            _mongodbInserter = mongodbInserter;
            _imageAdder = imageAdder;
        }

        static readonly string MONGO_URI = "mongodb://admin:StrongPassword123@mongodb:27017/?authSource=admin";
        static readonly string DB_NAME = "fires";
        static readonly string COLLECTION_NAME = "Fires";
        static readonly TimeZoneInfo KZ_TZ = TimeZoneInfo.FindSystemTimeZoneById("Asia/Qyzylorda");

        public async Task<(ConcurrentBag<BsonDocument>, ConcurrentBag<BsonDocument>)> parseAndFilterData(List<Dictionary<string, string>> allRecords)
        {

            var mongoClient = new MongoClient(MONGO_URI);
            var collection = mongoClient.GetDatabase(DB_NAME).GetCollection<BsonDocument>(COLLECTION_NAME);
            var existing = collection.Find(FilterDefinition<BsonDocument>.Empty)
                .Project(Builders<BsonDocument>.Projection.Include("latitude").Include("longitude").Include("sputnik_recorded_datetime"))
                .ToList()
                .Select(doc => (doc["latitude"].ToDouble(), doc["longitude"].ToDouble(), doc["sputnik_recorded_datetime"]))
                .ToHashSet();

            var existingNotFire = _NotFireCollection.Find(FilterDefinition<BsonDocument>.Empty)
                .Project(Builders<BsonDocument>.Projection.Include("latitude").Include("longitude").Include("sputnik_recorded_datetime"))
                .ToList()
                .Select(doc => (doc["latitude"].ToDouble(), doc["longitude"].ToDouble(), doc["sputnik_recorded_datetime"]))
                .ToHashSet();

            var now = TimeZoneInfo.ConvertTime(DateTime.Now, KZ_TZ);
            var apiRequestedStr = now.ToString("yyyy-MM-ddTHH:mm:ss+0500");

            var allFireRecords = new ConcurrentBag<BsonDocument>();
            var onlyFireRecords = new ConcurrentBag<BsonDocument>();

            await Parallel.ForEachAsync(allRecords, async (record, ct) =>
            {
                try
                {
                    Boolean isFire = true;
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
                    if (existingNotFire.Contains((lat, lon, dtKey))) return;

                    if (record.TryGetValue("instrument", out var instr))
                    {
                        if (instr == "MODIS") { record.Remove("bright_ti4"); record.Remove("bright_ti5"); }
                        else if (instr == "VIIRS") { record.Remove("brightness"); record.Remove("bright_t31"); }
                    }
                    record.Remove("type");

                    var data = new BsonDocument();
                    foreach (var kv in record)
                    {
                        var key = kv.Key;
                        var value = kv.Value;
                        if (key.Contains("datetime")) data[key] = value;
                        else if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double dbl))
                        {
                            if (value.Contains('.')) data[key] = dbl;
                            else if (int.TryParse(value, out int i)) data[key] = i;
                            else data[key] = dbl;
                        }
                        else data[key] = value;
                    }

                    //use mongodb
                    var oblast = await _helper.getRegion("oblast", lon, lat);
                    var raion = await _helper.getRegion("raion", lon, lat);

                    if (oblast != null)
                    {
                        var (oblastEn, oblastRu) = oblast.Value;
                        data["oblastEn"] = oblastEn;
                        data["oblastRu"] = oblastRu;
                    }
                    else
                    {
                        data["oblastEn"] = null;
                        data["oblastRu"] = null;
                    }


                    if (raion != null)
                    {
                        var (raionEn, raionRu) = raion.Value;
                        data["raionEn"] = raionEn;
                        data["raionRu"] = raionRu;
                    }
                    else
                    {
                        data["raionEn"] = null;
                        data["raionRu"] = null;
                    }
                    string? imageURL = null;

                    if (raion != null)
                    {
                        var (raionEn, raionRu) = raion.Value;
                        imageURL = await _imageAdder.addImage(raionRu, lat, lon);
                    }

                    if (imageURL != null)
                    {
                        data["imageUrl"] = imageURL;
                    }

                    string? result = await _helper.checkIfValidFire(lon, lat);
                    if (result != null)
                    {
                        data["source"] = "Not Fire";
                        await _mongodbInserter.insertOneDoc(data);
                        isFire = false;
                    }
                    else
                    {
                        data["source"] = "Fire";
                    }

                    data["telegram"] = "sent";

                    allFireRecords.Add(data);
                    if (isFire == true)
                    {
                        onlyFireRecords.Add(data);
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Skipping record due to error: {ex.Message}");
                }
            });
            return (allFireRecords, onlyFireRecords);
        }


    }
}
