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
    static readonly int Days = 2;
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
        { "West Kazakhstan Region", "Западно-Казахстанской области" },
        { "Aqtöbe region", "Актюбинской области" },
        { "Mangystau Region", "Мангистауской области" },
        { "East Kazakhstan Region", "Восточно-Казахстанской области" },
        { "Almaty Region", "Алматинской области" },
        { "Jambyl Region", "Жамбылской области" },
        { "Pavlodar Region", "Павлодарской области" },
        { "Karaganda Region", "Карагандинской области" },
        { "Kostanay Region", "Костанайской области" },
        { "Almaty", "городе Алматы" },
        { "Astana", "городе Астана" },
        { "Shymkent", "городе Шымкент" },
        { "Abay Region", "Абайской области" },
        { "Jetisu Region", "Жетысуской области" },
        { "Ulytau Region", "Улытауской области" },
        { "Turkistan Region", "Туркестанской области" },
        { "North Kazakhstan Region", "Северо-Казахстанской области" },
        { "Akmola Region", "Акмолинской области" },
        { "Kyzylorda Region", "Кызылординской области" },
        { "Atyrau Region", "Атырауской области" },

    };

    static readonly Dictionary<string, string> cityRussian = new Dictionary<string, string>
    {
        { "Arkalyk", "город Аркалык"},
        { "Rudny", "город Рудный"},
        { "Petropavl Public Administration", "город Петропавлск" },
        { "Priozersk", "город Приозёрск" },
        { "Kostanay", "город Костанай" },
        { "Karaganda", "город Караганда" },
        { "Taraz", "город Тараз" },
        { "Atyrau", "город Атырау"},
        { "Ridder", "город Риддер"},
        { "Oskemen", "город Усть-Каменогорск"},
        { "Taldykorgan", "город Талдыкорган"},
        { "Kyzylorda", "город Кызылорда"},
        { "Stepnogorsk", "город Степногорск"},
        { "Arys", "город Арыс"},
        { "Oral", "город Уральск"},
        { "Baikonur", "город Байконур"},
        { "Turkistan", "город Туркестан"},
        { "Kurchatov", "город Курчатов"},
        { "Pavlodar", "город Павлодар"},
        { "Aksu", "город Аксу"},
        { "Ekibastuz", "город Экибастуз"},
        { "Saran", "город Сарань"},
        { "Shaktinsk", "город Шахтинск"},
        { "Temirtau", "город Темиртау"},
        { "Karazhal", "город Каражал"},
        { "Balkhash", "город Балхаш"},
        { "Jezqazğan", "город Жезказган"},
        { "Satbayev", "город Сатпаев"},
        { "Aktau", "город Актау"},
        { "Zhanaozen", "город Жанаозен"},
        { "Qonaev", "город Конаев"},
        { "Tekeli", "город Текели"},
        { "Aktobe", "город Актобе"  },
        { "Kosshy C.A.", "город Косшы"},
        { "Alatau", "город Алатау"},
        { "Kentau", "город Кентау"},
        { "Semey", "город Семей"},
        { "Lisakovsk", "город Лисаковск"},
        { "Kokshetau", "город Кокшетау"},
    };

    static readonly Dictionary<string, string> raionRussian = new Dictionary<string, string>
    {
        { "Marqakol District", "район Маркакольский"},
        { "Ulken Naryn District", "район Улькен Нарынский"},
        { "Makanshi District", "район Маканчинский"},
        { "Aqswat District", "район Аксуатский"},
        { "Samar district", "район Самарский"},
        { "Keles District", "район Келесский"},
        { "Munaily District", "район Мунайлинский"},
        { "Aqqayıñ district", "район Аккайынский"},
        { "Gabit Musirepov District", "район Габита Мусрепова"},
        { "Türksib District", "район Турксибский"},
        { "Jetisu district", "район Жетысуский" },
        { "Zhanasemey District", "район Жанасемейский"},
        { "Bostandıq District", "район Бостандыкский"},
        { "Altai District", "район Алтайский"},
        { "Oiyl District", "район Уилский"},
        { "Birzhan sal District", "район Биржан сал"},
        { "Sauran District", "район Сауранский"},
        { "Sarıarqa district", "район Сарыаркинский"},
        { "Nauryzbay District", "район Наурызбайский"},
        { "Ile District", "район Илийский" },
        { "Sarqan District", "район Саркандский"},
        { "Eskeldi District", "район Ескельдинский"},
        { "Turan district", "район Туранский"},
        { "Al-Farabi District", "район Аль-Фарабийский"},
        { "Enbekshinsky district", "район Енбекшинский"},
        { "Qarataw district", "район Каратауский"},
        { "Bayqoñır district", "район Байконурский"},
        { "Zhetisay District", "район Жетысайский"},
        { "Kegen District", "район Кегенский"},
        { "Alatau District", "район Алатауский" },
        { "Medeu District", "район Медеуский" },
        { "Almaly District", "район Алмалинский" },
        { "Auezov District", "район Ауэзовский" },
        { "Kurmangazy district", "район Курмангазинский"},
        { "Balqaş District", "район Балхашский" },
        { "Almaty District", "район Алматинский" },
        { "Alaköl District", "район Алакольский" },
        { "Enbekshikazakh District", "район Енбекшиказахский" },
        { "Qarasay District", "район Карасайский" },
        { "Qaratal District", "район Каратальский" },
        { "Kerbulaq District", "район Кербулакский" },
        { "Köksu District", "район Коксуский" },
        { "Panfilov District", "район Панфиловский" },
        { "Rayımbek district", "район Райымбекский" },
        { "Talgar District", "район Талгарский" },
        { "Uygur District", "район Уйгурский" },
        { "Akkol District", "район Аккольский" },
        { "Arşalı district", "район Аршалынский" },
        { "Astrakhan District", "район Астраханский" },
        { "Atbasar District", "район Атбасарский" },
        { "Bulandy district", "район Буландынский" },
        { "Egindiköl district", "район Егиндыкольский" },
        { "Ereymentau District", "район Ерейментауский" },
        { "Qorğaljın district", "район Коргалжынский" },
        { "Sandyqtau district", "район Сандыктауский" },
        { "Şortandı district", "район Шортандинский" },
        { "Burabay District", "район Бурабайский" },
        { "Äyteke bï District", "район Айтекебийский" },
        { "Alğa district", "район Алгинский" },
        { "Zharkain District", "район Жаркаинский" },
        { "Zhaksy District", "район Жаксынский" },
        { "Zerendi District", "район Зерендинский" },
        { "Celïnograd", "район Целиноградский" },
        { "Ïnder district", "район Индерский" },
        { "Temir District", "район Темирский" },
        { "Şalqar District", "район Шалкарский" },
        { "Muğaljar district", "район Мугалжарский" },
        { "Martök district", "район Мартукский" },
        { "Xromtaw district", "район Хромтауский" },
        { "Qobda district", "район Хобдинский" },
        { "Yrgyz District", "район Иргизский" },
        { "Bayğanïn District", "район Байганинский" },
        { "Qarğaly district", "район Каргалинский" },
        { "Isatay District", "район Исатайский" },
        { "Kyzylkoga District", "район Кзылкогинский" },
        { "Makat District", "район Макатский" },
        { "Makhambet District", "район Махамбетский" },
        { "Zhylyoi District", "район Жылыойский" },
        { "Abay District", "район Абайский" },
        { "Ayagöz District", "район Аягозский" },
        { "Borodulikha District", "район Бородулихинский" },
        { "Beskaragay District", "район Бескарагайский" },
        { "Glubokoe District", "район Глубоковский" },
        { "Katonkaragay District", "район Катон-Карагайский" },
        { "Kökpekti district", "район Кокпектинский" },
        { "Kurshim district", "район Курчумский" },
        { "Shemonaikha District", "район Шемонаихинский" },
        { "Tarbağatay district", "район Тарбагатайский" },
        { "Ulan District", "район Уланский" },
        { "Urzhar District", "район Урджарский" },
        { "Zaysan District", "район Зайсанский" },
        { "Jarma district", "район Жарминский" },
        { "Aqtoğay District", "район Актогайский" },
        { "Jelezïn district", "район Железинский" },
        { "Uspen District", "район Успенский" },
        { "Sharbaqty District", "район Щербактинский" },
        { "Pavlodar District", "район Павлодарский" },
        { "May District", "район Майский" },
        { "Aqqwlı district", "район Аккулинский" },
        { "Tereñköl District", "район Теренкольский" },
        { "Bayanaul District", "район Баянаульский" },
        { "Aqsu District", "район Аксуский" },
        { "Ualikhanov District", "район Уалихановский" },
        { "Timiryazev District", "район Тимирязевский" },
        { "Tayınşa District", "район Тайыншинский" },
        { "Shal akyn District", "район Шал Акына" },
        { "Mamlyut District", "район Мамлютский" },
        { "Esil District", "район Есильский" },
        { "Mağjan Jumabaev District", "район Магжана Жумабаева" },
        { "Aiyrtau District", "район Айыртауский" },
        { "Qaraqïya District", "район Каракиянский" },
        { "Beyneu District", "район Бейнеуский" },
        { "Ertis district", "район Иртышский" },
        { "Mangystau District", "район Мангистауский" },
        { "Tüpqarağan District", "район Тупкараганский" },
        { "Aqjar district", "район Акжарский" },
        { "Qyzyljar District", "район Кызылжарский" },
        { "Beimbet Mailin District", "район Беимбета Майлина" },
        { "Sarykol District", "район Сарыкольский" },
        { "Qostanay District", "район Костанайский" }, 
        { "Nauyrzym District", "район Наурзумский" },
        { "Mendykara District", "район Мендыкаринский" },
        { "Karasu District", "район Карасуский" },
        { "Qarabalyq District", "район Карабалыкский" },
        { "Qamysty district", "район Камыстинский" },
        { "Fyodorov District", "район Фёдоровский" },
        { "Zhangeldin District", "район Джангельдинский" },
        { "Denisov district", "район Денисовский" },
        { "Auliekol District", "район Аулиекольский" },
        { "Amangeldi District", "район Амангельдинский" },
        { "Altynsarin District", "район Алтынсаринский" },
        { "Janaarqa District", "район Жанааркинский" },
        { "Ulytau District", "район Улытауский" },
        { "Shet District", "район Шетский" },
        { "Osakarov District", "район Осакаровский" },
        { "Nura District", "район Нуринский" },
        { "Qarqaralı district", "район Каркаралинский" },
        { "Buqar Jıraw District", "район Бухар-Жырауский" },
        { "Akzhaik District", "район Акжаикский" },
        { "Tülkibas District", "район Тюлькубасский" },
        { "Tole Bi District", "район Толебийский" },
        { "Sozak District", "район Сузакский" },
        { "Sayram District", "район Сайрамский" },
        { "Sarıağaş District", "район Сарыагашский" },
        { "Otyrar District", "район Отырарский" },
        { "Ordabasy District", "район Ордабасинский" },
        { "Maqtaaral District", "район Мактааральский" },
        { "Kazygurt District", "район Казыгуртский" },
        { "Şardara District", "район Шардаринский" },
        { "Baydibek District", "район Байдибека" },
        { "Jañaqorğan District", "район Жанакорганский" },
        { "Jalağaş District", "район Жалагашский" },
        { "Sırdarïya District", "район Сырдарьинский" },
        { "Şïeli District", "район Шиелийский" },
        { "Qazalı district", "район Казалинский" },
        { "Karmakshy District", "район Кармакшинский" },
        { "Aral District", "район Аральский" },
        { "Jitiqara district", "район Житикаринский" },
        { "Uzınköl District", "район Узункольский" },
        { "Zhualy District", "район Жуалынский" },    
        { "Zhambyl District", "район Жамбылский" },
        { "Jambyl District", "район Жамбылский" },
        { "Talas District", "район Таласский" },
        { "Shu District", "район Шуский" },
        { "Sarysu District", "район Сарысуский" },
        { "Moyınqum District", "район Мойынкумский" },
        { "Merki District", "район Меркенский" },
        { "Turar Ryskulov District", "район Т. Рыскулова" },
        { "Qorday District", "район Кордайский" },
        { "Bayzaq District", "район Байзакский" },
        { "Baiterek District", "район Байтерекский" },
        { "Bokey Orda District", "район Бокейординский" },
        { "Terekti District", "район Теректинский" },
        { "Taskala District", "район Таскалинский" },
        { "Syrym District", "район Сырымский" },
        { "Kaztal District", "район Казталовский" },
        { "Karatobe District", "район Каратобинский" },
        { "Zhanybek District", "район Жанибекский" },
        { "Zhanakala District", "район Жангалинский" },
        { "Şıñğırlaw district", "район Чингирлауский" },
        { "Börli district", "район Бурлинский" },
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

        var url1 = "https://services3.arcgis.com/RGg2rzCtnLDgGhvB/arcgis/rest/services/Административные_границы_Казахстана/FeatureServer/0/query";
        var urlFireOrNot = "https://services3.arcgis.com/RGg2rzCtnLDgGhvB/arcgis/rest/services/export/FeatureServer/0/query";

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

                var admin = await client.PostAsync(url1, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"f", "json"},
                    {"geometry", geometry},
                    {"geometryType", "esriGeometryPoint"},
                    {"inSR", "4326"},
                    {"spatialRel", "esriSpatialRelIntersects"},
                    {"outFields", "name_en, oblast_en"},
                    {"returnGeometry", "false"},
                    {"token", token}
                }));

                using var doc = JsonDocument.Parse(await admin.Content.ReadAsStringAsync());
                var features = doc.RootElement.GetProperty("features");
                if (features.GetArrayLength() == 0) return;

                var raion = features[0].GetProperty("attributes").GetProperty("name_en").ToString();
                var oblast = features[0].GetProperty("attributes").GetProperty("oblast_en").ToString();

                typed["oblast"] = oblast;
                typed["raion"] = raion;

                typed["telegram"] = "sent";

                typedRecords.Add(typed);

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Skipping record due to error: {ex.Message}");
            }
        });

        var FireFilteredRecords = new ConcurrentBag<BsonDocument>();
        if (typedRecords.Any())
        {
            Console.WriteLine("start the foreach loop");
            foreach (var typedRecord in typedRecords)
            {
                Boolean isFire = false;
                string oblast = typedRecord["oblast"].ToString()!;
                string raion = typedRecord["raion"].ToString()!;

                string oblastName = oblastRussian.ContainsKey(oblast) ? oblastRussian[oblast] : oblast;
                string raionName;
                if (raionRussian.ContainsKey(raion))
                {
                    raionName = raionRussian[raion];
                }
                else if (cityRussian.ContainsKey(raion))
                {
                    raionName = cityRussian[raion];
                }
                else
                {
                    raionName = raion;
                }

                string latitudeIn = typedRecord["latitude"].ToDouble().ToString("0.#####", CultureInfo.InvariantCulture);
                string longitudeIn = typedRecord["longitude"].ToDouble().ToString("0.#####", CultureInfo.InvariantCulture);

                string geometryFireOrNot = $"{longitudeIn}, {latitudeIn}";

                var adminFireOrNot = await client.PostAsync(urlFireOrNot, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    {"f", "json"},
                    {"geometry", geometryFireOrNot},
                    {"geometryType", "esriGeometryPoint"},
                    {"inSR", "4326"},
                    {"spatialRel", "esriSpatialRelIntersects"},
                    {"outFields", "*"},
                    {"returnGeometry", "false"},
                    {"token", token}
                }));

                using var docFireOrNot = JsonDocument.Parse(await adminFireOrNot.Content.ReadAsStringAsync());
                var featuresFireOrNot = docFireOrNot.RootElement.GetProperty("features");
                string source = "Месторождение";
                if (featuresFireOrNot.GetArrayLength() == 0)
                {
                    source = "Пожар";
                    isFire = true;
                }

                string str_api_datetime = typedRecord["api_requested_datetime"].ToString()!;
                string str_sputnik_datetime = typedRecord["sputnik_recorded_datetime"].ToString()!;

                string api_requested_date = $"{str_api_datetime.Substring(8, 2)}.{str_api_datetime.Substring(5, 2)}.{str_api_datetime.Substring(0, 4)}";
                string api_requested_time = str_api_datetime.Substring(11, 8);
                string sputnik_recorded_date = $"{str_sputnik_datetime.Substring(8, 2)}.{str_sputnik_datetime.Substring(5, 2)}.{str_sputnik_datetime.Substring(0, 4)}";
                string sputnik_recorded_time = str_sputnik_datetime.Substring(11, 8);

                string message = $"Обнаружена высокая температура в {oblastName}, {raionName}, зафиксированный спутником в {sputnik_recorded_date}, {sputnik_recorded_time}. " + "\n"
                  + $"Данные получены нами в {api_requested_date}, {api_requested_time}:";

                string coords = latitudeIn + "," + longitudeIn;

                string visibleUrl2 = $"https://www.google.com/maps/search/?api=1&query={coords}";

                message += $"\nСсылка: {visibleUrl2}";

                message += $"\nИсточник: {source}";

                foreach (var field in typedRecord)
                {
                    if (field.Name == "country_id" || field.Name == "oblast" || field.Name == "raion"
                        || field.Name == "sputnik_recorded_datetime" || field.Name == "api_requested_datetime" || field.Name=="telegram") continue;

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
                if (isFire == true)
                {
                    FireFilteredRecords.Add(typedRecord);
                }
            }
        }
        else
        {
            Console.WriteLine("No records to insert. Skipping MongoDB update.");
        }

        if (FireFilteredRecords.Any())
        {
            await collection.InsertManyAsync(FireFilteredRecords);
            Console.WriteLine($"Inserted {FireFilteredRecords.Count} documents at {apiRequestedStr}");
        }
        else
        {
            Console.WriteLine("No fire records to insert. Skipping MongoDB update.");
        }
    }
}
