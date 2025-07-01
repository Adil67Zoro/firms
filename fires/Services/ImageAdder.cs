using MongoDB.Bson;
using MongoDB.Driver;
using NetTopologySuite.Geometries;
using NetTopologySuite.Simplify;
using System.Globalization;

namespace fires.Services
{
    public class ImageAdder
    {
        private readonly IMongoCollection<BsonDocument> _RaionCollection;

        public ImageAdder(IConfiguration config)
        {
            var client = new MongoClient(config.GetConnectionString("Default"));
            var database = client.GetDatabase("fires");
            _RaionCollection = database.GetCollection<BsonDocument>("Raion");
        }

        public async Task<String?> addImage(string raionName, double fireLat, double fireLon)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("nameRu", raionName);
            var raionDoc = await _RaionCollection.Find(filter).FirstOrDefaultAsync();

            if (raionDoc == null)
            {
                Console.WriteLine("Raion not found.");
                return null;
            }

            var geometry = raionDoc["geometry"].AsBsonDocument;
            var type = geometry["type"].AsString;
            var coords = geometry["coordinates"].AsBsonArray;

            var polylines = new List<string>();
            double totalMinLon = double.MaxValue, totalMaxLon = double.MinValue;
            double totalMinLat = double.MaxValue, totalMaxLat = double.MinValue;
            double tolerance = 0.02;

            if (type == "Polygon")
            {
                var ringArray = coords[0].AsBsonArray;
                var ringPoints = ExtractCoordinates(ringArray);
                AddPolyline(ringPoints, polylines, tolerance,
                       ref totalMinLon, ref totalMaxLon, ref totalMinLat, ref totalMaxLat);
            }
            else if (type == "MultiPolygon")
            {
                foreach (var polygonArray in coords)
                {
                    var ringArray = polygonArray.AsBsonArray[0].AsBsonArray;
                    var ringPoints = ExtractCoordinates(ringArray);
                    AddPolyline(ringPoints, polylines, tolerance,
                        ref totalMinLon, ref totalMaxLon, ref totalMinLat, ref totalMaxLat);
                }
            }
            else
            {
                Console.WriteLine($"Unsupported geometry type: {type}");
                return null;
            }

            double centerLon = (totalMinLon + totalMaxLon) / 2;
            double centerLat = (totalMinLat + totalMaxLat) / 2;

            double bboxWidthDeg = totalMaxLon - totalMinLon;
            double bboxHeightDeg = totalMaxLat - totalMinLat;

            bboxWidthDeg *= 1.2;
            bboxHeightDeg *= 1.2;

            int imageWidth = 600;
            int imageHeight = 400;

            double zoomX = Math.Log2(360.0 * (imageWidth / 256.0) / bboxWidthDeg);
            double zoomY = Math.Log2(180.0 * (imageHeight / 256.0) / bboxHeightDeg);

            int zoom = (int)Math.Floor(Math.Min(zoomX, zoomY));

            string url = $"https://static-maps.yandex.ru/1.x/" +
                         $"?ll={centerLon.ToString(CultureInfo.InvariantCulture)},{centerLat.ToString(CultureInfo.InvariantCulture)}" +
                         $"&z={zoom}&l=sat&size=600,400" +
                         $"&pt={fireLon.ToString(CultureInfo.InvariantCulture)},{fireLat.ToString(CultureInfo.InvariantCulture)},pm2rdm";

            foreach (var pl in polylines)
            {
                url += $"&pl={pl}";
            }

            return url;
        }


        void AddPolyline(List<Coordinate> coords, List<string>? polylines, double tolerance,
                        ref double totalMinLon, ref double totalMaxLon, ref double totalMinLat, ref double totalMaxLat)
        {
            if (!coords.First().Equals2D(coords.Last()))
                coords.Add(coords.First());

            var ring = new LinearRing(coords.ToArray());
            var polygon = new Polygon(ring);
            var simplified = TopologyPreservingSimplifier.Simplify(polygon, tolerance) as Polygon;

            if (simplified.Coordinates.Length <= 100)
            {
                var pl = string.Join(",", simplified.Coordinates.Select(p =>
                    $"{p.X.ToString(CultureInfo.InvariantCulture)},{p.Y.ToString(CultureInfo.InvariantCulture)}"));
                polylines.Add(pl);

                totalMinLon = Math.Min(totalMinLon, simplified.Coordinates.Min(p => p.X));
                totalMaxLon = Math.Max(totalMaxLon, simplified.Coordinates.Max(p => p.X));
                totalMinLat = Math.Min(totalMinLat, simplified.Coordinates.Min(p => p.Y));
                totalMaxLat = Math.Max(totalMaxLat, simplified.Coordinates.Max(p => p.Y));
            }
        }

        List<Coordinate> ExtractCoordinates(BsonArray array)
        {
            var list = new List<Coordinate>();
            foreach (var c in array)
            {
                var pair = c.AsBsonArray;
                double lon = pair[0].ToDouble();
                double lat = pair[1].ToDouble();
                list.Add(new Coordinate(lon, lat));
            }
            return list;
        }
    }
}
