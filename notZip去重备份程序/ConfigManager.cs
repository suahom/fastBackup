using System.Text.Json;

static class ConfigManager
{
    public static (string SourceDir, string DestDir) ReadConfig()
    {
        string configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.json");

        if (!File.Exists(configPath))
        {
            throw new FileNotFoundException("配置文件 config.json 未找到。", configPath);
        }

        string jsonString = File.ReadAllText(configPath);
        using JsonDocument doc = JsonDocument.Parse(jsonString);
        JsonElement root = doc.RootElement;

        string sourceDir = root.GetProperty("SourcePath").GetString();
        string destDir = root.GetProperty("DestinationPath").GetString();

        if (string.IsNullOrEmpty(sourceDir) || string.IsNullOrEmpty(destDir))
        {
            throw new InvalidOperationException("配置文件中的 SourcePath 或 DestinationPath 无效。");
        }

        return (sourceDir, destDir);
    }
}
