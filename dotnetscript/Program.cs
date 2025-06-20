using CsvHelper;
using CsvHelper.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Text.Json;

class Program
{
    static readonly string MAP_KEY = "7db0ce61ad6b61b0f85ddd76641e1df6";
    static readonly string MONGO_URI = "mongodb://admin:StrongPassword123@mongodb:27017/?authSource=admin";
    static readonly string DB_NAME = "fires";
    static readonly string COLLECTION_NAME = "Fires";
    static readonly int Days = 1;
    static readonly TimeZoneInfo KZ_TZ = TimeZoneInfo.FindSystemTimeZoneById("Asia/Qyzylorda");
    static readonly HttpClient client = new();

    static readonly string BOTTOKEN = "7964975789:AAEol85f8N1DgJ_gN67lBUFazAXxC75NDHQ";
    static readonly string CHANNELLINK = "@firms_fire_channel";



    static readonly List<string> Urls = new()
    {
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/MODIS_NRT/KAZ/{Days}",
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/KAZ/{Days}",
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_NRT/KAZ/{Days}",
        $"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_SP/KAZ/{Days}"
    };

    static readonly Dictionary<string, string> fieldsDict = new Dictionary<string, string>
    {
        { "latitude", "Широта, градусы" },
        { "longitude", "Долгота, градусы" },
        { "scan", "Размер пикселя по сканированию, градусы" },
        { "track", "Размер пикселя по следу спутника, градусы" },
        { "satellite", "Спутник" },
        { "instrument", "Прибор" },
        { "confidence", "Достоверность, проценты" },
        { "bright_ti5", "Яркостная температура канала I-5, Кельвины" },
        { "bright_ti4", "Яркостная температура канала I-4, Кельвины" },
        { "frp", "Радиационная мощность, Мегаватты" },
        { "daynight", "День/ночь" },
        { "brightness", "Яркостная температура канала 21/22, Кельвины" },
        { "bright_t31", "Яркостная температура канала 31, Кельвины" },
        { "version", "Версия обработки" },
        { "D", "дневное обнаружение" },
        { "N", "ночное обнаружение" }
    };

    static readonly Dictionary<string, string> viirsConfidenceDict = new Dictionary<string, string>
    {
        { "l", "низкая" },
        { "n", "номинальная" },
        { "h", "высокая" }
    };




    static readonly Dictionary<string, string> oblastRussian = new Dictionary<string, string>
    {
        { "Almaty", "Алматинской" },
        { "Aqmola", "Акмолинской" },
        { "Aqtöbe", "Актюбинской" },
        { "Atyrau", "Атырауской" },
        { "East Kazakhstan", " Восточно-Казахстанской" },
        { "Mangghystau", "Мангистауской" },
        { "North Kazakhstan", "Северо-Казахстанской" },
        { "Pavlodar", "Павлодарской" },
        { "Qaraghandy", "Карагандинской" },
        { "Qostanay", "Костанайской" },
        { "Qyzylorda", "Кызылординской " },
        { "South Kazakhstan", "Туркестанской" },
        { "West Kazakhstan", " Западно-Казахстанской" },
        { "Zhambyl", " Жамбылской" },
    };

    static readonly Dictionary<string, string> raionRussian = new Dictionary<string, string>
    {
        { "Balkhashskiy", "Балхашский" },
        { "Almaty (Alma-Ata)", "Алматинский" },
        { "Alakolskiy", "Алакольский" },
        { "Enbekshikazakhskiy", "Енбекшиказахский" },
        { "Karasayskiy", "Карасайский" },
        { "Karatal`skiy", "Каратальский" },
        { "Kerbulakskiy", "Кербулакский" },
        { "Koksuskiy", "Коксуский" },
        { "Panfilovskiy", "Панфиловский" },
        { "Raiymbekskiy", "Райымбекский" },
        { "Talgarskiy", "Талгарский" },
        { "Uygurskiy", "Уйгурский" },
        { "Akkol`skiy", "Аккольский" },
        { "Arshalynskiy", "Аршалынский" },
        { "Astrakhansiy", "Астраханский" },
        { "Atbasarskiy", "Атбасарский" },
        { "Bulandynskiy", "Буландынский" },
        { "Egindykol`skiy", "Егиндыкольский" },
        { "Enbekshil`derskiy", "Енбекшилдерский" },
        { "Ereymengauskiy", "Ерейментауский" },
        { "Korgalzhynskiy", "Коргалжынский" },
        { "Sandyktauskiy", "Сандыктауский" },
        { "Shortandinskiy", "Шортандинский" },
        { "Shuchinskiy", "Бурабайский" },
        { "Aytekebiyskiy", "Айтекебийский" },
        { "Alginskiy", "Алгинский" },
        { "Zharkainskiy", "Жаркаинский" },
        { "Zhaksynskiy", "Жаксынский" },
        { "Zerendinskiy", "Зерендинский" },
        { "Tselinogradskiy", "Целиноградский" },
        { "Inderskiy", "Индерский" },
        { "Temirskiy", "Темирский" },
        { "Shalkarskiy", "Шалкарский" },
        { "Mugalzharskiy", "Мугалжарский" },
        { "Martukskiy", "Мартукский" },
        { "Khromtauskiy", "Хромтауский" },
        { "Khobdinskiy", "Хобдинский" },
        { "Irgizskiy", "Иргизский" },
        { "Bayganinskiy", "Байганинский" },
        { "Kargalinskiy", "Каргалинский" },
        { "Isatayskiy", "Исатайский" },
        { "Kurmangazinskiy", "Курмангазинский" },
        { "Kzylkoginskiy", "Кзылкогинский" },
        { "Makatskiy", "Макатский" },
        { "Makhambetskiy", "Махамбетский" },
        { "Zhylyoyskiy", "Жылыойский" },
        { "Abayskiy", "Абайский" },
        { "Ayagozskiy", "Аягозский" },
        { "Borodulikhinskiy", "Бородулихинский" },
        { "Beskaragayskiy", "Бескарагайский" },
        { "Glubokovskiy", "Глубоковский" },
        { "Katon-Karagayskiy", "Катон-Карагайский" },
        { "Kokpektinskiy", "Кокпектинский" },
        { "Kurchumskiy", "Курчумский" },
        { "Shemonaikhinskiy", "Шемонаихинский" },
        { "Tarbagatayskiy", "Тарбагатайский" },
        { "Ulanskiy", "Уланский" },
        { "Urdzharskiy", "Урджарский" },
        { "Zaysanskiy", "Зайсанский" },
        { "Zharminskiy", "Жарминский" },
        { "Aktogayskiy", "Актогайский" },
        { "Zhelezinskiy", "Железинский" },
        { "Uspenskiy", "Успенский" },
        { "Sherbaktinskiy", "Щербактинский" },
        { "Pavlodarskiy", "Павлодарский" },
        { "Mayskiy", "Майский" },
        { "Lebyazhinskiy", "Аккулинский" },
        { "Kachirskiy", "Теренкольский" },
        { "Bayanaul`skiy", "Баянаульский" },
        { "Aksuskiy", "Аксуский" },
        { "Ualikhanovskiy", "Уалихановский" },
        { "Timiryazevskiy", "Тимирязевский" },
        { "Taiynshinskiy", "Тайыншинский" },
        { "Shal Akyna", "Шал Акына" },
        { "Mamlyutskiy", "Мамлютский" },
        { "Esil`skiy", "Есильский" },
        { "Bulaevskiy", "Магжана Жумабаева" },
        { "Ayyrtauskiy", "Айыртауский" },
        { "Karakiyanskiy", "Каракиянский" },
        { "Beyneuskiy", "Бейнеуский" },
        { "Irtyshskiy", "Иртышский" },
        { "Manghystauskiy", "Мангистауский" },
        { "Tupkaraganskiy", "Тупкараганский" },
        { "Akzharskiy", "Акжарский" },
        { "Kyzylzharskiy", "Кызылжарский" },
        { "Taranovskiy", "Беимбета Майлина" },
        { "Sarykol`skiy", "Сарыкольский" },
        { "Qostanay", "Костанайский" },
        { "Naurzumskiy", "Наурзумский" },
        { "Mendykarinskiy", "Мендыкаринский" },
        { "Karasuskiy", "Карасуский" },
        { "Karabalykskiy", "Карабалыкский" },
        { "Kamystinskiy", "Камыстинский" },
        { "Fyodorovskiy", "Фёдоровский" },
        { "Dzhangil`dinskiy", "Джангельдинский" },
        { "Denisovskiy", "Денисовский" },
        { "Auliekol`skiy", "Аулиекольский" },
        { "Amangel`dinskiy", "Амангельдинский" },
        { "Altynsarinskiy", "Алтынсаринский" },
        { "Zhanaarkinskiy", "Жанааркинский" },
        { "Ulytauskiy", "Улытауский" },
        { "Shetskiy", "Шетский" },
        { "Osakarovskiy", "Осакаровский" },
        { "Nurinskiy", "Нуринский" },
        { "Karkaralinskiy", "Каркаралинский" },
        { "Bukhar-Zhyrauskiy", "Бухар-Жырауский" },
        { "Akzhaikskiy", "Акжаикский" },
        { "Tyul`kubaskiy", "Тюлькубасский" },
        { "Tolebiyskiy", "Толебийский" },
        { "Suzakskiy", "Сузакский" },
        { "Sayramskiy", "Сайрамский" },
        { "Saryagashskiy", "Сарыагашский" },
        { "Otrarskiy", "Отырарский" },
        { "Ordabasynskiy", "Ордабасинский" },
        { "Maktaaral`skiy", "Мактааральский" },
        { "Kazygurtskiy", "Казыгуртский" },
        { "Chardarinskiy", "Шардаринский" },
        { "Baydibekskiy", "Байдибека" },
        { "Zhanakorganskiy", "Жанакорганский" },
        { "Zhalagashskiy", "Жалагашский" },
        { "Terenozekskiy", "Сырдарьинский" },
        { "Shieliyskiy", "Шиелийский" },
        { "Kazalinskiy", "Казалинский" },
        { "Karmakchinskiy", "Кармакшинский" },
        { "Aral`skiy", "Аральский" },
        { "Zhitikarinskiy", "Житикаринский" },
        { "Uzunkol`skiy", "Узункольский" },
        { "Zhualynskiy", "Жуалынский" },
        { "Zhualy", "Жуалыйский" },
        { "Zhambylskiy", "Жамбылский" },
        { "Zhamb", "Жамбылский" },
        { "Talasskiy", "Таласский" },
        { "Shuskiy", "Шуский" },
        { "Sarysuskiy", "Сарысуский" },
        { "Moyynkumskiy", "Мойынкумский" },
        { "Merkenskiy", "Меркенский" },
        { "Lugovskoy", "Т. Рыскулова" },
        { "Kordayskiy", "Кордайский" },
        { "Bayzakskiy", "Байзакский" },
        { "Zelenovskiy", "Байтерекский" },
        { "Urdinskiy", "Бокейординский" },
        { "Terektinskiy", "Теректинский" },
        { "Taskalinskiy", "Таскалинский" },
        { "Syrymskiy", "Сырымский" },
        { "Kaztalovskiy", "Казталовский" },
        { "Karatobinskiy", "Каратобинский" },
        { "Dzhanybekskiy", "Жанибекский" },
        { "Dzhangalinskiy", "Жангалинский" },
        { "Chingirlauskiy", "Чингирлауский" },
        { "Burlinskiy", "Бурлинский" },
    };



    static async Task Main(string[] args)
    {
        while (true)
        {
            await FetchAndUpload();
            Thread.Sleep(TimeSpan.FromMinutes(10));
        }
    }

    static async Task FetchAndUpload()
    {
        var allRecords = new List<Dictionary<string, string>>();

        var contentToken = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            { "username", "adiltest" },
            { "password", "y!6iyT59B9k.Hth" },
            { "referer", "https://www.arcgis.com" },
            { "f", "json"}
        });

        var responseToken = await client.PostAsync("https://www.arcgis.com/sharing/rest/generateToken", contentToken);
        var token = JsonDocument.Parse(await responseToken.Content.ReadAsStringAsync())
            .RootElement.GetProperty("token").GetString()!;

        var url1 = "https://services3.arcgis.com/RGg2rzCtnLDgGhvB/arcgis/rest/services/KZ_Regions/FeatureServer/1/query";
        var url2 = "https://services3.arcgis.com/RGg2rzCtnLDgGhvB/arcgis/rest/services/KZ_Regions/FeatureServer/2/query";

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

        if (!allRecords.Any())
        {
            Console.WriteLine("No data fetched from any source. Skipping MongoDB update.");
            return;
        }

        var mongoClient = new MongoClient(MONGO_URI);
        var collection = mongoClient.GetDatabase(DB_NAME).GetCollection<BsonDocument>(COLLECTION_NAME);
        var existing = collection.Find(FilterDefinition<BsonDocument>.Empty)
            .Project(Builders<BsonDocument>.Projection.Include("latitude").Include("longitude").Include("sputnik_recorded_datetime"))
            .ToList()
            .Select(doc => (doc["latitude"].ToDouble(), doc["longitude"].ToDouble(), doc["sputnik_recorded_datetime"]))
            .ToHashSet();

        var now = TimeZoneInfo.ConvertTime(DateTime.Now, KZ_TZ);
        var apiRequestedStr = now.ToString("yyyy-MM-ddTHH:mm:ss+0500");
        var typedRecords = new ConcurrentBag<BsonDocument>();

        await Parallel.ForEachAsync(allRecords, async (record, ct) =>
        {
            try
            {
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

                if (record.TryGetValue("instrument", out var instr))
                {
                    if (instr == "MODIS") { record.Remove("bright_ti4"); record.Remove("bright_ti5"); }
                    else if (instr == "VIIRS") { record.Remove("brightness"); record.Remove("bright_t31"); }
                }
                record.Remove("type");

                var typed = new BsonDocument();
                foreach (var kv in record)
                {
                    var key = kv.Key;
                    var value = kv.Value;
                    if (key.Contains("datetime")) typed[key] = value;
                    else if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double dbl))
                    {
                        if (value.Contains('.')) typed[key] = dbl;
                        else if (int.TryParse(value, out int i)) typed[key] = i;
                        else typed[key] = dbl;
                    }
                    else typed[key] = value;
                }

                string geometry = $"{record["longitude"]}, {record["latitude"]}";

                var admin1 = await client.PostAsync(url1, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"f", "json"},
                    {"geometry", geometry},
                    {"geometryType", "esriGeometryPoint"},
                    {"inSR", "4326"},
                    {"spatialRel", "esriSpatialRelIntersects"},
                    {"outFields", "NAME_1"},
                    {"returnGeometry", "false"},
                    {"token", token}
                }));

                var admin2 = await client.PostAsync(url2, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"f", "json"},
                    {"geometry", geometry},
                    {"geometryType", "esriGeometryPoint"},
                    {"inSR", "4326"},
                    {"spatialRel", "esriSpatialRelIntersects"},
                    {"outFields", "NAME_2"},
                    {"returnGeometry", "false"},
                    {"token", token}
                }));

                using var doc1 = JsonDocument.Parse(await admin1.Content.ReadAsStringAsync());
                using var doc2 = JsonDocument.Parse(await admin2.Content.ReadAsStringAsync());
                var features1 = doc1.RootElement.GetProperty("features");
                var features2 = doc2.RootElement.GetProperty("features");
                if (features1.GetArrayLength() == 0 || features2.GetArrayLength() == 0) return;

                typed["oblast"] = features1[0].GetProperty("attributes").GetProperty("NAME_1").GetString();
                typed["raion"] = features2[0].GetProperty("attributes").GetProperty("NAME_2").GetString();
                typed["telegram"] = "sent";

                typedRecords.Add(typed);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Skipping record due to error: {ex.Message}");
            }
        });

        if (typedRecords.Any())
        {
            Console.WriteLine("start the foreach loop");
            foreach (var typedRecord in typedRecords)
            {
                string oblast = typedRecord["oblast"].ToString()!;
                string raion = typedRecord["raion"].ToString()!;

                string oblastName = oblastRussian.ContainsKey(oblast) ? oblastRussian[oblast] : oblast;
                string raionName = raionRussian.ContainsKey(raion) ? raionRussian[raion] : raion;

                string str_api_datetime = typedRecord["api_requested_datetime"].ToString()!;
                string str_sputnik_datetime = typedRecord["sputnik_recorded_datetime"].ToString()!;

                string api_requested_date = $"{str_api_datetime.Substring(8, 2)}.{str_api_datetime.Substring(5, 2)}.{str_api_datetime.Substring(0, 4)}";
                string api_requested_time = str_api_datetime.Substring(11, 8);
                string sputnik_recorded_date = $"{str_sputnik_datetime.Substring(8, 2)}.{str_sputnik_datetime.Substring(5, 2)}.{str_sputnik_datetime.Substring(0, 4)}";
                string sputnik_recorded_time = str_sputnik_datetime.Substring(11, 8);

                string message = $"Обнаружен пожар в {oblastName} области, район {raionName}, зафиксированный спутником в {sputnik_recorded_date}, {sputnik_recorded_time}. " + "\n"
                  + $"Данные получены нами в {api_requested_date}, {api_requested_time}:";

                string latitudeIn = typedRecord["latitude"].ToDouble().ToString("0.#####", CultureInfo.InvariantCulture);
                string longitudeIn = typedRecord["longitude"].ToDouble().ToString("0.#####", CultureInfo.InvariantCulture);

                string coords = latitudeIn + "," + longitudeIn;

                string visibleUrl2 = $"https://www.google.com/maps/search/?api=1&query={coords}";

                message += $"\nСсылка: {visibleUrl2}";

                foreach (var field in typedRecord)
                {
                    if (field.Name == "country_id" || field.Name == "oblast" || field.Name == "raion"
                        || field.Name == "sputnik_recorded_datetime" || field.Name == "api_requested_datetime") continue;

                    string nameStr;
                    nameStr = fieldsDict.ContainsKey(field.Name) ? fieldsDict[field.Name] : field.Name;

                    string valueStr;
                    if (field.Value.IsDouble)
                    {
                        double val = field.Value.AsDouble;
                        valueStr = field.Name switch
                        {
                            "latitude" or "longitude" => val.ToString("0.#####", CultureInfo.InvariantCulture),
                            "scan" or "track" or "brightness" or "bright_t31" or "bright_ti4" or "bright_ti5" or "frp" => val.ToString("0.##", CultureInfo.InvariantCulture),
                            _ => val.ToString("G", CultureInfo.InvariantCulture)
                        };
                    }
                    else if (field.Value.IsInt32)
                        valueStr = field.Value.AsInt32.ToString();
                    else if (field.Value.IsInt64)
                        valueStr = field.Value.AsInt64.ToString();
                    else
                        valueStr = field.Value.ToString()!;

                    if (typedRecord["instrument"] == "VIIRS" && field.Name == "confidence")
                    {
                        valueStr = viirsConfidenceDict[valueStr!];
                    }

                    if (field.Name == "daynight")
                    {
                        valueStr = fieldsDict[valueStr];
                    }

                    message += $"\n{nameStr}: {valueStr}";
                }


                bool sent = false;
                while (!sent)
                {
                    var url = $"https://api.telegram.org/bot{BOTTOKEN}/sendMessage?chat_id={CHANNELLINK}&text={WebUtility.UrlEncode(message)}";
                    var res = await client.GetAsync(url);
                    var body = await res.Content.ReadAsStringAsync();

                    if (res.StatusCode == HttpStatusCode.OK)
                    {
                        Console.WriteLine("Message sent");
                        sent = true;
                        await Task.Delay(1000);
                    }
                    else if ((int)res.StatusCode == 429)
                    {
                        Console.WriteLine("Rate limit hit. Waiting...");
                        try
                        {
                            using var json = JsonDocument.Parse(body);
                            if (json.RootElement.TryGetProperty("parameters", out var parameters) &&
                                parameters.TryGetProperty("retry_after", out var retry))
                            {
                                int seconds = retry.GetInt32();
                                Console.WriteLine($"Retry after {seconds} seconds...");
                                await Task.Delay(TimeSpan.FromSeconds(seconds));
                            }
                            else
                            {
                                Console.WriteLine("Retry_after not found. Waiting 30 seconds by default.");
                                await Task.Delay(TimeSpan.FromSeconds(30));
                            }
                        }
                        catch
                        {
                            Console.WriteLine("Error parsing retry_after. Waiting 30 seconds.");
                            await Task.Delay(TimeSpan.FromSeconds(30));
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Failed to send message. Status: {res.StatusCode}, Body: {body}");
                        sent = true;
                    }
                }
            }

            await collection.InsertManyAsync(typedRecords);
            Console.WriteLine($"Inserted {typedRecords.Count} documents at {apiRequestedStr}");
        }
        else
        {
            Console.WriteLine("No records to insert. Skipping MongoDB update.");
        }
    }
}
