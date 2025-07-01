using MongoDB.Bson;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Text.Json;

namespace fires.Services
{
    public class TelegramSender
    {
        private readonly Helper _helper;
        public TelegramSender(Helper helper)
        {
            _helper = helper;
        }


        static readonly Dictionary<string, string> fieldsDict = new Dictionary<string, string>
        {
            { "latitude", "Широта, градусы" },
            { "longitude", "Долгота, градусы" },
            { "scan", "Размер пикселя по сканированию, градусы" },
            { "track", "Размер пикселя по следу спутника, градусы" },
            { "satellite", "Спутник" },
            { "instrument", "Прибор" },
            { "confidence", "Достоверность, проценты" },
            { "bright_ti5", "Яркостная температура канала I-5, градусы Цельсия" },
            { "bright_ti4", "Яркостная температура канала I-4, градусы Цельсия" },
            { "frp", "Радиационная мощность, Мегаватты" },
            { "daynight", "День/ночь" },
            { "brightness", "Яркостная температура канала 21/22, градусы Цельсия" },
            { "bright_t31", "Яркостная температура канала 31, градусы Цельсия" },
            { "version", "Версия обработки" },
            { "D", "дневное обнаружение" },
            { "N", "ночное обнаружение" },
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

        private static string BOTTOKEN = "7964975789:AAEol85f8N1DgJ_gN67lBUFazAXxC75NDHQ";
        private static string CHANNELLINK = "@firms_fire_channel";
        static readonly HttpClient client = new();

        public async Task sendToTelegram(ConcurrentBag<BsonDocument> FireFilteredData)
        {
            var FireFilteredRecords = new ConcurrentBag<BsonDocument>();
            foreach (var FireRecord in FireFilteredData)
            {
                string oblastEn = FireRecord["oblastEn"].ToString()!;

                string raionRu = FireRecord["raionRu"].ToString()!;

                string oblastName = oblastRussian.ContainsKey(oblastEn) ? oblastRussian[oblastEn] : null;

                string latitudeIn = FireRecord["latitude"].ToDouble().ToString("0.#####", CultureInfo.InvariantCulture);
                string longitudeIn = FireRecord["longitude"].ToDouble().ToString("0.#####", CultureInfo.InvariantCulture);

                string str_api_datetime = FireRecord["api_requested_datetime"].ToString()!;
                string str_sputnik_datetime = FireRecord["sputnik_recorded_datetime"].ToString()!;

                string api_requested_date = $"{str_api_datetime.Substring(8, 2)}.{str_api_datetime.Substring(5, 2)}.{str_api_datetime.Substring(0, 4)}";
                string api_requested_time = str_api_datetime.Substring(11, 8);
                string sputnik_recorded_date = $"{str_sputnik_datetime.Substring(8, 2)}.{str_sputnik_datetime.Substring(5, 2)}.{str_sputnik_datetime.Substring(0, 4)}";
                string sputnik_recorded_time = str_sputnik_datetime.Substring(11, 8);
                string message = "";
                if (FireRecord["source"].ToString()!.Equals("Fire"))
                {
                    message += "🔥";
                }
                else
                {
                    message += "🏭";
                }

                message += $"Обнаружена высокая температура в {oblastName}, {raionRu}, зафиксированный спутником в {sputnik_recorded_date}, {sputnik_recorded_time}. " + "\n"
                      + $"Данные получены нами в {api_requested_date}, {api_requested_time}:";

                string coords = latitudeIn + "," + longitudeIn;

                string visibleUrl2 = $"https://www.google.com/maps/search/?api=1&query={coords}";

                message += $"\nСсылка: {visibleUrl2}";

                foreach (var field in FireRecord)
                {
                    if (field.Name == "raionRu" || field.Name == "raionEn" || field.Name == "oblastRu" ||
                        field.Name == "oblastEn" || field.Name == "country_id" || field.Name == "oblast" ||
                        field.Name == "raion" || field.Name == "sputnik_recorded_datetime"
                        || field.Name == "api_requested_datetime" || field.Name == "telegram"
                        || field.Name == "imageUrl" || field.Name == "source") continue;

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

                    if (FireRecord["instrument"] == "VIIRS" && field.Name == "confidence")
                    {
                        valueStr = viirsConfidenceDict[valueStr!];
                    }

                    if (field.Name == "daynight")
                    {
                        valueStr = fieldsDict[valueStr];
                    }

                    if (field.Name == "brightness" || field.Name == "bright_t31" || field.Name == "bright_ti4" || field.Name == "bright_ti5")
                    {
                        if (field.Value.IsInt32)
                        {
                            valueStr = (field.Value.AsInt32 - 273.15).ToString("0.##", CultureInfo.InvariantCulture);
                        }
                        else if (field.Value.IsInt64)
                        {
                            valueStr = (field.Value.AsInt64 - 273.15).ToString("0.##", CultureInfo.InvariantCulture);
                        }
                        else if (field.Value.IsDouble)
                        {
                            valueStr = (field.Value.AsDouble - 273.15).ToString("0.##", CultureInfo.InvariantCulture);
                        }
                    }

                    message += $"\n{nameStr}: {valueStr}";
                }


                bool sent = false;
                while (!sent)
                {
                    string? imageUrl = FireRecord.TryGetValue("imageUrl", out var img) ? img.AsString : null;

                    HttpResponseMessage res;
                    if (!string.IsNullOrEmpty(imageUrl))
                    {
                        var content = new MultipartFormDataContent
                        {
                            { new StringContent(CHANNELLINK), "chat_id" },
                            { new StringContent(message), "caption" },
                            { new StringContent(imageUrl), "photo" }
                        };
                        res = await client.PostAsync($"https://api.telegram.org/bot{BOTTOKEN}/sendPhoto", content);
                    }
                    else
                    {
                        var url = $"https://api.telegram.org/bot{BOTTOKEN}/sendMessage?chat_id={CHANNELLINK}&text={WebUtility.UrlEncode(message)}";
                        res = await client.GetAsync(url);
                    }

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
        }

    }
}
