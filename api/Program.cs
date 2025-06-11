using api.Mappings;
using api.Models;
using api.Models.Entities;
using api.Services;
using MongoDB.Bson.Serialization;

var builder = WebApplication.CreateBuilder(args);

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
