using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;

namespace fires.Services
{
    public class MongodbInserter
    {
        private readonly IMongoCollection<BsonDocument> _FiresCollection;
        private readonly IMongoCollection<BsonDocument> _NotFireCollection;

        public MongodbInserter(IConfiguration config)
        {
            var client = new MongoClient(config.GetConnectionString("Default"));
            var database = client.GetDatabase("fires");
            _FiresCollection = database.GetCollection<BsonDocument>("Fires");
            _NotFireCollection = database.GetCollection<BsonDocument>("NotFireData");
        }

        public async Task insertDocs(ConcurrentBag<BsonDocument> FireFilteredRecords)
        {
            await _FiresCollection.InsertManyAsync(FireFilteredRecords);
            return;
        }

        public async Task insertOneDoc(BsonDocument NotFireDocument)
        {
            await _NotFireCollection.InsertOneAsync(NotFireDocument);
            return;
        }
    }
}
