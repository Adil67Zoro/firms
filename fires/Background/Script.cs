using fires.Services;

namespace fires.Background
{
    public class Script
    {
        private readonly DataFetcher dataFetcher;
        private readonly DataParser dataParser;
        private readonly MongodbInserter mongodbInserter;
        private readonly Helper regionFinder;
        private readonly TelegramSender telegramSender;

        public Script(DataFetcher DataFetcher, DataParser DataParser,
            MongodbInserter MongodbInserter, Helper RegionFinder, TelegramSender TelegramSender)
        {
            dataFetcher = DataFetcher;
            dataParser = DataParser;
            mongodbInserter = MongodbInserter;
            regionFinder = RegionFinder;
            telegramSender = TelegramSender;
        }

        public async Task Main(string[] args)
        {
            while (true)
            {
               await FetchAndUpload();
               Thread.Sleep(TimeSpan.FromMinutes(10));
            }
        }

        public async Task FetchAndUpload()
        {
            var allData = await dataFetcher.FetchAllData();
            Console.WriteLine(allData.Count);
            var Data = await dataParser.parseAndFilterData(allData);
            var AllFireData = Data.Item1;
            var OnlyFireData = Data.Item2;
            Console.WriteLine(OnlyFireData.Count);

            if (!AllFireData.IsEmpty)
            {
                await telegramSender.sendToTelegram(AllFireData);
            }

            if (!OnlyFireData.IsEmpty)
            {
                await mongodbInserter.insertDocs(OnlyFireData);
            }


        }
    }
}
