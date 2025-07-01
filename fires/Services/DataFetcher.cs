using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace fires.Services
{
    public class DataFetcher
    {
        static readonly string MAP_KEY = "7db0ce61ad6b61b0f85ddd76641e1df6";
        static readonly int Days = 2;
        static readonly TimeZoneInfo KZ_TZ = TimeZoneInfo.FindSystemTimeZoneById("Asia/Qyzylorda");

        static readonly List<string> Urls = new()
        {
            $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/MODIS_NRT/KAZ/{Days}",
            $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/KAZ/{Days}",
            $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_NRT/KAZ/{Days}",
            $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_SP/KAZ/{Days}"
        };

        public async Task<List<Dictionary<string, string>>> FetchAllData()
        {
            HttpClient client = new();
            var allRecords = new List<Dictionary<string, string>>();

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

            return allRecords;
        }
    }
}
