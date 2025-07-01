using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Text.Json;

namespace fires.Controllers
{
    [ApiController]
    [Route("api/geojson")]
    public class GeoJsonController : ControllerBase
    {
        private readonly IMongoCollection<BsonDocument> _InvalidCollection;

        public GeoJsonController(IConfiguration config)
        {
            var client = new MongoClient(config.GetConnectionString("Default"));
            var database = client.GetDatabase("fires");
            _InvalidCollection = database.GetCollection<BsonDocument>("Invalid");
        }

        [HttpPost("upload/{level}")]
        public async Task<IActionResult> UploadGeoJson([FromForm] IFormFile file, [FromRoute] string level)
        {
            if (file == null || file.Length == 0)
                return BadRequest("No file uploaded.");

            using var reader = new StreamReader(file.OpenReadStream());
            var jsonContent = await reader.ReadToEndAsync();

            using var doc = JsonDocument.Parse(jsonContent);
            List<BsonDocument> documents = new();

            foreach (var feature in doc.RootElement.GetProperty("features").EnumerateArray())
            {
                var geometryJson = feature.GetProperty("geometry").GetRawText();
                var geometryDoc = BsonDocument.Parse(geometryJson);
                var name = feature.GetProperty("properties").GetProperty("description").GetString();

                var newDoc = new BsonDocument
                    {
                        { "name", name },
                        { "geometry", geometryDoc }
                    };

                documents.Add(newDoc);
            }

            await _InvalidCollection.InsertManyAsync(documents);

            return Ok("GeoJSON uploaded and stored successfully.");
        }

    }
}
