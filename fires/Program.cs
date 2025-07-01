using fires.Background;
using fires.Services;
using fires.Controllers;
using MongoDB.Driver;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls("http://0.0.0.0:5000");

var mongoConnectionString = builder.Configuration.GetConnectionString("Default");
builder.Services.AddSingleton<IMongoClient>(sp => new MongoClient(mongoConnectionString));


builder.Services.AddSingleton<ImageAdder>();
builder.Services.AddSingleton<GeoJsonController>();
builder.Services.AddSingleton<DataFetcher>();
builder.Services.AddSingleton<DataParser>();
builder.Services.AddSingleton<MongodbInserter>();
builder.Services.AddSingleton<Helper>();
builder.Services.AddSingleton<TelegramSender>();
builder.Services.AddSingleton<Script>();

builder.Services.AddHostedService<ScriptRunner>();

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

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
