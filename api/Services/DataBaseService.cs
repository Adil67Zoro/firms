using Microsoft.Extensions.Options;
using MongoDB.Driver;
using api.Models;
using api.Models.Entities;
using Microsoft.AspNetCore.Mvc;
using api.Models.DTOs;
using MongoDB.Bson.Serialization.IdGenerators;
using AutoMapper;

namespace api.Services
{
    public class DataBaseService
    {
        private readonly IMongoCollection<Data> _dataCollection;
        private readonly IMapper _mapper;

        public DataBaseService(
            IMongoClient mongoClient,
            IOptions<DataDatabaseSettings> dataDatabaseSettings,
            IMapper mapper)
        {
            var mongoDatabase = mongoClient.GetDatabase(
                dataDatabaseSettings.Value.DatabaseName);

            _dataCollection = mongoDatabase.GetCollection<Data>(
                dataDatabaseSettings.Value.FiresCollectionName);

            _mapper = mapper;
        }

        public async Task<List<DataDto>> GetLatestInMinAsync(int minutes)
        {
            var now = DateTimeOffset.UtcNow.ToOffset(TimeSpan.FromHours(5));
            var fromTime = now.AddMinutes(-minutes);
            
            var allData = await _dataCollection.Find(_ => true).ToListAsync();

            var filtered = allData
                .Where(d =>
                {
                    var str = d.Api_Requested_DateTime;

                    if (DateTimeOffset.TryParseExact(str, "yyyy-MM-ddTHH:mm:sszzz", null, System.Globalization.DateTimeStyles.None, out var dto))
                    {
                        return dto >= fromTime;
                    }

                    return false;
                })
                .ToList();

            return _mapper.Map<List<DataDto>>(filtered);
        }
    }
}
