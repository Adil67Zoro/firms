namespace fires.Background
{
    public class ScriptRunner : BackgroundService
    {
        private readonly Script _script;

        public ScriptRunner(Script script)
        {
            _script = script;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _script.Main(Array.Empty<string>());
        }
    }
}
