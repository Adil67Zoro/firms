namespace api.Models
{
    public class DataDatabaseSettings
    {
        public string ConnectionString { get; set; } = null!;

        public string DatabaseName { get; set; } = null!;

        public string FiresCollectionName { get; set; } = null!;
    }
}
