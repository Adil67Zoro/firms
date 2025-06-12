using api.Mappings;
using api.Models;
using api.Models.Entities;
using api.Services;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls("http://0.0.0.0:5000");


if (!BsonClassMap.IsClassMapRegistered(typeof(Data)))
{
    BsonClassMap.RegisterClassMap<Data>(cm =>
    {
        cm.AutoMap();

        cm.GetMemberMap(d => d.Bright_ti4)
            .SetShouldSerializeMethod(obj => ((Data)obj).ShouldSerializeBright_ti4());

        cm.GetMemberMap(d => d.Bright_ti5)
            .SetShouldSerializeMethod(obj => ((Data)obj).ShouldSerializeBright_ti5());

        cm.GetMemberMap(d => d.Bright_t31)
            .SetShouldSerializeMethod(obj => ((Data)obj).ShouldSerializeBright_t31());

        cm.GetMemberMap(d => d.Brightness)
            .SetShouldSerializeMethod(obj => ((Data)obj).ShouldSerializeBrightness());
    });
}

var mongoConnectionString = builder.Configuration.GetConnectionString("Default");
builder.Services.AddSingleton<IMongoClient>(sp => new MongoClient(mongoConnectionString));


builder.Services.Configure<DataDatabaseSettings>(
    builder.Configuration.GetSection("DataDatabase"));

builder.Services.AddSingleton<DataBaseService>();

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddAutoMapper(typeof(MappingProfile));

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
