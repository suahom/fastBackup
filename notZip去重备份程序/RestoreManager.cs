using Microsoft.Data.Sqlite;
using notZip去重备份程序;

static class RestoreManager
{
    private const string DB_FILE_NAME = "file_info.db";

    public static async Task PerformRestore()
    {
        var (sourceDir, destDir) = ConfigManager.ReadConfig();
        string dbFilePath = Path.Combine(destDir, DB_FILE_NAME);

        Console.WriteLine($"开始从 {destDir} 还原到 {sourceDir}...");

        try
        {
            Directory.CreateDirectory(sourceDir);
            await RestoreFolderAsync(destDir, sourceDir, dbFilePath);
            Console.WriteLine("还原完成！");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"还原过程中发生错误: {ex.Message}");
        }
    }

    private static async Task RestoreFolderAsync(string backupDir, string restoreDir, string dbFile)
    {
        using (var connection = new SqliteConnection($"Data Source={dbFile}"))
        {
            await connection.OpenAsync();

            var filesToRestore = await GetFilesForRestoreAsync(connection);
            var duplicateFiles = await GetDuplicateFilesAsync(connection);

            int totalFiles = filesToRestore.Count + duplicateFiles.Count;
            var progressReporter = new ProgressReporter(totalFiles);

            foreach (var filePath in filesToRestore)
            {
                await RestoreFileAsync(backupDir, restoreDir, filePath, connection);
                progressReporter.ReportProgress();
            }

            foreach (var (originalPath, duplicatePath) in duplicateFiles)
            {
                await RestoreDuplicateFileAsync(backupDir, restoreDir, originalPath, duplicatePath, connection);
                progressReporter.ReportProgress();
            }

            progressReporter.Complete();
        }
    }

    private static async Task RestoreFileAsync(string backupDir, string restoreDir, string filePath, SqliteConnection connection)
    {
        var destPath = Path.Combine(restoreDir, filePath);
        var sourcePath = Path.Combine(backupDir, filePath);

        Directory.CreateDirectory(Path.GetDirectoryName(destPath));

        if (File.Exists(sourcePath))
        {
            File.Copy(sourcePath, destPath, true);
            await RestoreFileMetadataAsync(connection, filePath, destPath);
            Console.WriteLine($"已还原文件: {filePath}");
        }
        else
        {
            Console.WriteLine($"警告: 源文件不存在，无法还原: {sourcePath}");
        }
    }

    private static async Task RestoreDuplicateFileAsync(string backupDir, string restoreDir, string originalPath, string duplicatePath, SqliteConnection connection)
    {
        var destPath = Path.Combine(restoreDir, originalPath);
        var sourcePath = Path.Combine(backupDir, duplicatePath );

        Directory.CreateDirectory(Path.GetDirectoryName(destPath));

        if (File.Exists(sourcePath))
        {
            File.Copy(sourcePath, destPath, true);
            await RestoreFileMetadataAsync(connection, duplicatePath, destPath);
            Console.WriteLine($"已还原重复文件: {duplicatePath} (原始文件: {originalPath})");
        }
        else
        {
            Console.WriteLine($"警告: 原始文件不存在，无法还原重复文件: {sourcePath}");
        }
    }

    private static async Task<List<string>> GetFilesForRestoreAsync(SqliteConnection connection)
    {
        var files = new List<string>();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT FilePath FROM FileMetadata";
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    files.Add(reader.GetString(0));
                }
            }
        }

        return files;
    }

    private static async Task<List<(string OriginalPath, string DuplicatePath)>> GetDuplicateFilesAsync(SqliteConnection connection)
    {
        var duplicates = new List<(string OriginalPath, string DuplicatePath)>();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT OriginalPath, DuplicatePath FROM DuplicateFiles";
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    string originalPath = reader.GetString(0);
                    string duplicatePath = reader.GetString(1);
                    duplicates.Add((originalPath, duplicatePath));
                }
            }
        }

        return duplicates;
    }

    private static async Task RestoreFileMetadataAsync(SqliteConnection connection, string filePath, string physicalPath)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT CreationTime, LastWriteTime, LastAccessTime, Attributes FROM FileMetadata WHERE FilePath = @path";
            command.Parameters.AddWithValue("@path", filePath);

            using (var reader = await command.ExecuteReaderAsync())
            {
                if (await reader.ReadAsync())
                {
                    DateTime creationTime = DateTime.Parse(reader.GetString(0));
                    DateTime lastWriteTime = DateTime.Parse(reader.GetString(1));
                    DateTime lastAccessTime = DateTime.Parse(reader.GetString(2));
                    FileAttributes attributes = (FileAttributes)Enum.Parse(typeof(FileAttributes), reader.GetString(3));

                    File.SetCreationTimeUtc(physicalPath, creationTime);
                    File.SetLastWriteTimeUtc(physicalPath, lastWriteTime);
                    File.SetLastAccessTimeUtc(physicalPath, lastAccessTime);
                    File.SetAttributes(physicalPath, attributes);
                }
            }
        }
    }
}