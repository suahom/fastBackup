using Microsoft.Data.Sqlite;
using notZip去重备份程序;
using System.Security.Cryptography;

static class BackupManager
{
    private const string DB_FILE_NAME = "file_info.db";
    private const int CURRENT_DB_VERSION = 2;
    private const int HASH_THRESHOLD = 100 * 1024; // 100KB
    private const int PARTIAL_HASH_SIZE = 30 * 1024; // 30KB

    public static async Task PerformBackup()
    {
        var (sourceDir, destDir) = ConfigManager.ReadConfig();
        string dbFilePath = Path.Combine(destDir, DB_FILE_NAME);

        if (Path.GetFullPath(sourceDir).Equals(Path.GetFullPath(destDir), StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("源目录和目标目录不能相同。");
        }

        Console.WriteLine($"开始备份 {sourceDir} 到 {destDir}...");

        try
        {
            Directory.CreateDirectory(destDir);
            await InitializeOrUpdateDatabaseAsync(dbFilePath);
            await BackupFolderAsync(sourceDir, destDir, dbFilePath);
            Console.WriteLine("备份完成！");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"备份过程中发生错误: {ex.Message}");
            Console.WriteLine($"错误详情: {ex}");
        }
    }

    private static async Task BackupFolderAsync(string sourceDir, string destDir, string dbFile)
    {
        using (var connection = new SqliteConnection($"Data Source={dbFile}"))
        {
            await connection.OpenAsync();

            var filesToBackup = await GetFilesForBackupAsync(sourceDir, connection);
            var progressReporter = new ProgressReporter(filesToBackup.Count);

            foreach (var (relativePath, uniquePath) in filesToBackup)
            {
                var originalPath = Path.Combine(sourceDir, relativePath);
                var destPath = Path.Combine(destDir, relativePath);
                // 跳过在根目录与数据库文件重名的文件
                if (Path.GetFileName(destPath).Equals(DB_FILE_NAME, StringComparison.OrdinalIgnoreCase) &&
                Path.GetDirectoryName(destPath).Equals(destDir, StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"跳过与数据库同名的文件: {relativePath}");
                    continue;
                }

                Directory.CreateDirectory(Path.GetDirectoryName(destPath));

                if (relativePath == uniquePath)
                {
                    File.Copy(originalPath, destPath, true);
                    await StoreFileMetadataAsync(connection, relativePath, new FileInfo(originalPath));
                }
                else
                {
                    await StoreDuplicateInfoAsync(connection, relativePath, uniquePath);
                }

                progressReporter.ReportProgress();
            }

            progressReporter.Complete();
        }
    }

    private static async Task InitializeOrUpdateDatabaseAsync(string dbFilePath)
    {
        using (var connection = new SqliteConnection($"Data Source={dbFilePath}"))
        {
            await connection.OpenAsync();

            // Check if the version table exists
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='DbVersion'";
                var result = await command.ExecuteScalarAsync();

                if (result == null)
                {
                    // Create version table and set initial version
                    command.CommandText = @"
                        CREATE TABLE DbVersion (Version INTEGER);
                        INSERT INTO DbVersion (Version) VALUES (0);";
                    await command.ExecuteNonQueryAsync();
                }
            }

            // Check current version and update if necessary
            int currentVersion = await GetDatabaseVersionAsync(connection);
            if (currentVersion < CURRENT_DB_VERSION)
            {
                await UpdateDatabaseSchemaAsync(connection, currentVersion);
            }
        }
    }

    private static async Task<int> GetDatabaseVersionAsync(SqliteConnection connection)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT Version FROM DbVersion";
            var result = await command.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
    }

    private static async Task UpdateDatabaseSchemaAsync(SqliteConnection connection, int currentVersion)
    {
        Console.WriteLine($"更新数据库结构从版本 {currentVersion} 到 {CURRENT_DB_VERSION}");

        using (var transaction = connection.BeginTransaction())
        {
            try
            {
                using (var command = connection.CreateCommand())
                {
                    if (currentVersion < 1)
                    {
                        command.CommandText = @"
                            CREATE TABLE IF NOT EXISTS FileHashes (
                                FilePath TEXT PRIMARY KEY,
                                FileHash TEXT NOT NULL
                            );
                            CREATE TABLE IF NOT EXISTS DuplicateFiles (
                                OriginalPath TEXT NOT NULL,
                                DuplicatePath TEXT NOT NULL,
                                PRIMARY KEY (OriginalPath, DuplicatePath)
                            );
                            CREATE TABLE IF NOT EXISTS FileMetadata (
                                FilePath TEXT PRIMARY KEY,
                                CreationTime TEXT NOT NULL,
                                LastWriteTime TEXT NOT NULL,
                                LastAccessTime TEXT NOT NULL,
                                Attributes TEXT NOT NULL
                            );";
                        await command.ExecuteNonQueryAsync();
                    }

                    if (currentVersion < 2)
                    {
                        // 版本2的更改（如果有的话）
                    }

                    if (currentVersion < 3)
                    {
                        // 添加 FileSize 列到 FileMetadata 表
                        command.CommandText = "ALTER TABLE FileMetadata ADD COLUMN FileSize INTEGER NOT NULL DEFAULT 0";
                        await command.ExecuteNonQueryAsync();
                    }

                    // 更新数据库版本
                    command.CommandText = "UPDATE DbVersion SET Version = @version";
                    command.Parameters.AddWithValue("@version", CURRENT_DB_VERSION);
                    await command.ExecuteNonQueryAsync();
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"更新数据库结构时发生错误: {ex.Message}");
                transaction.Rollback();
                throw;
            }
        }
    }





    private static async Task<Dictionary<string, string>> GetFilesForBackupAsync(string sourceDir, SqliteConnection connection)
    {
        var allFiles = Directory.GetFiles(sourceDir, "*.*", SearchOption.AllDirectories);
        var fileMapping = new Dictionary<string, string>();

        foreach (var file in allFiles)
        {
            var fileInfo = new FileInfo(file);
            var relativePath = Path.GetRelativePath(sourceDir, file);

            var existingFile = await FindExistingFileAsync(connection, fileInfo);

            if (existingFile == null)
            {
                // New unique file
                var hash = await CalculateFileHashAsync(file);
                await StoreFileInfoAsync(connection, relativePath, hash, fileInfo);
                fileMapping[relativePath] = relativePath;
            }
            else
            {
                // Potential duplicate file
                fileMapping[relativePath] = existingFile;
            }
        }

        return fileMapping;
    }

    private static async Task<string> FindExistingFileAsync(SqliteConnection connection, FileInfo fileInfo)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                SELECT FilePath 
                FROM FileMetadata 
                WHERE LastWriteTime = @lastWriteTime 
                AND FileSize = @fileSize";
            command.Parameters.AddWithValue("@lastWriteTime", fileInfo.LastWriteTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@fileSize", fileInfo.Length);

            var result = await command.ExecuteScalarAsync();

            if (result != null)
            {
                string existingFilePath = result.ToString();
                if (await CompareFileHashesAsync(connection, fileInfo.FullName, existingFilePath))
                {
                    return existingFilePath;
                }
            }

            return null;
        }
    }

    private static async Task<bool> CompareFileHashesAsync(SqliteConnection connection, string newFile, string existingFile)
    {
        string newHash = await CalculateFileHashAsync(newFile);
        string existingHash = await GetStoredHashAsync(connection, existingFile);

        return newHash == existingHash;
    }

    private static async Task<string> GetStoredHashAsync(SqliteConnection connection, string filePath)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT FileHash FROM FileHashes WHERE FilePath = @path";
            command.Parameters.AddWithValue("@path", filePath);
            return (await command.ExecuteScalarAsync())?.ToString();
        }
    }

    private static async Task<string> CalculateFileHashAsync(string filePath)
    {
        using (var md5 = MD5.Create())
        using (var stream = File.OpenRead(filePath))
        {
            byte[] hash;
            if (stream.Length <= HASH_THRESHOLD)
            {
                hash = await md5.ComputeHashAsync(stream);
            }
            else
            {
                byte[] buffer = new byte[PARTIAL_HASH_SIZE * 3];

                // Read first 30KB
                await stream.ReadAsync(buffer, 0, PARTIAL_HASH_SIZE);

                // Read middle 30KB
                stream.Seek(stream.Length / 2 - PARTIAL_HASH_SIZE / 2, SeekOrigin.Begin);
                await stream.ReadAsync(buffer, PARTIAL_HASH_SIZE, PARTIAL_HASH_SIZE);

                // Read last 30KB
                stream.Seek(-PARTIAL_HASH_SIZE, SeekOrigin.End);
                await stream.ReadAsync(buffer, PARTIAL_HASH_SIZE * 2, PARTIAL_HASH_SIZE);

                hash = md5.ComputeHash(buffer);
            }

            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }
    }

    private static async Task StoreFileInfoAsync(SqliteConnection connection, string filePath, string hash, FileInfo fileInfo)
    {
        using (var transaction = connection.BeginTransaction())
        {
            try
            {
                // Store file hash
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "INSERT OR REPLACE INTO FileHashes (FilePath, FileHash) VALUES (@path, @hash)";
                    command.Parameters.AddWithValue("@path", filePath);
                    command.Parameters.AddWithValue("@hash", hash);
                    await command.ExecuteNonQueryAsync();
                }

                // Store file metadata
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT OR REPLACE INTO FileMetadata 
                        (FilePath, CreationTime, LastWriteTime, LastAccessTime, Attributes, FileSize)
                        VALUES (@path, @creationTime, @lastWriteTime, @lastAccessTime, @attributes, @fileSize)";
                    command.Parameters.AddWithValue("@path", filePath);
                    command.Parameters.AddWithValue("@creationTime", fileInfo.CreationTimeUtc.ToString("o"));
                    command.Parameters.AddWithValue("@lastWriteTime", fileInfo.LastWriteTimeUtc.ToString("o"));
                    command.Parameters.AddWithValue("@lastAccessTime", fileInfo.LastAccessTimeUtc.ToString("o"));
                    command.Parameters.AddWithValue("@attributes", fileInfo.Attributes.ToString());
                    command.Parameters.AddWithValue("@fileSize", fileInfo.Length);
                    await command.ExecuteNonQueryAsync();
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }



    private static async Task StoreFileMetadataAsync(SqliteConnection connection, string filePath, FileInfo fileInfo)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                INSERT OR REPLACE INTO FileMetadata (FilePath, CreationTime, LastWriteTime, LastAccessTime, Attributes)
                VALUES (@path, @creationTime, @lastWriteTime, @lastAccessTime, @attributes)";
            command.Parameters.AddWithValue("@path", filePath);
            command.Parameters.AddWithValue("@creationTime", fileInfo.CreationTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@lastWriteTime", fileInfo.LastWriteTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@lastAccessTime", fileInfo.LastAccessTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@attributes", fileInfo.Attributes.ToString());
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task StoreDuplicateInfoAsync(SqliteConnection connection, string originalPath, string duplicatePath)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "INSERT OR IGNORE INTO DuplicateFiles (OriginalPath, DuplicatePath) VALUES (@original, @duplicate)";
            command.Parameters.AddWithValue("@original", originalPath);
            command.Parameters.AddWithValue("@duplicate", duplicatePath);
            await command.ExecuteNonQueryAsync();
        }
    }

}
