using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;

namespace fires.Services
{
    public class Helper
    {
        private readonly IMongoCollection<BsonDocument> _OblastCollection;
        private readonly IMongoCollection<BsonDocument> _RaionCollection;
        private readonly IMongoCollection<BsonDocument> _InvalidCollection;

        public Helper(IConfiguration config)
        {
            var client = new MongoClient(config.GetConnectionString("Default"));
            var database = client.GetDatabase("fires");
            _OblastCollection = database.GetCollection<BsonDocument>("Oblast");
            _RaionCollection = database.GetCollection<BsonDocument>("Raion");
            _InvalidCollection = database.GetCollection<BsonDocument>("Invalid");
        }


        public async Task<(String, String)?> getRegion(string level, double longitude, double latitude)
        {
            var point = new GeoJsonPoint<GeoJson2DCoordinates>(
                new GeoJson2DCoordinates(longitude, latitude)
            );
            BsonDocument? result = null;
            var filter = Builders<BsonDocument>.Filter.GeoIntersects("geometry", point);
            if (level.Equals("oblast"))
            {
                result = await _OblastCollection.Find(filter).FirstOrDefaultAsync();
            }
            else if (level.Equals("raion"))
            {
                result = await _RaionCollection.Find(filter).FirstOrDefaultAsync();
            }


            if (result != null)
                return ((string)result["nameEn"], (string)result["nameRu"]);
            else
                return null;

        }

        public async Task<String?> checkIfValidFire(double longitude, double latitude)
        {
            var point = new GeoJsonPoint<GeoJson2DCoordinates>(
               new GeoJson2DCoordinates(longitude, latitude)
            );
            BsonDocument? result = null;
            var filter = Builders<BsonDocument>.Filter.GeoIntersects("geometry", point);
            result = await _InvalidCollection.Find(filter).FirstOrDefaultAsync();
            if (result != null)
            {
                return (string)result["name"];
            }
            else
            {
                return null;
            }
        }

    }
}
