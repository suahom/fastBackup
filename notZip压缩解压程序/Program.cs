using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using System.Diagnostics;

Console.WriteLine("压缩：输入1；解压：输入2");
string input = Console.ReadLine();
int result;

if (int.TryParse(input, out result))
{
    switch (result)
    {
        case 1:
            Console.WriteLine("您选择了压缩操作");
            // 调用压缩方法
            await ProgramZip.Main(null);
            break;
        case 2:
            Console.WriteLine("您选择了解压操作");
            // 调用解压方法
            await ProgramNotzip.Main(null);
            break;
        default:
            Console.WriteLine("无效的选择，请输入1或2");
            break;
    }
}
else
{
    Console.WriteLine("输入无效，请输入数字1或2");
}

class ProgramNotzip
{
    private const string COMPRESSED_FILE_NAME = "compressed.zip";
    private const string DB_FILE_NAME = "file_info.db";
    private static List<(string source, string destination)> _pendingFiles = new List<(string, string)>();

    internal static async Task Main(string[] args)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        string currentDirectory = Directory.GetCurrentDirectory();
        string compressedFilePath = Path.Combine(currentDirectory, COMPRESSED_FILE_NAME);
        string dbFilePath = Path.Combine(currentDirectory, DB_FILE_NAME);

        if (!File.Exists(compressedFilePath))
        {
            Console.WriteLine($"Error: {COMPRESSED_FILE_NAME} not found in the current directory.");
            return;
        }

        if (!File.Exists(dbFilePath))
        {
            Console.WriteLine($"Error: {DB_FILE_NAME} not found in the current directory.");
            return;
        }

        Console.WriteLine($"Starting decompression of {COMPRESSED_FILE_NAME}...");

        try
        {
            await DecompressFolderAsync(compressedFilePath, currentDirectory, dbFilePath);
            await ProcessPendingFilesAsync(dbFilePath);
            Console.WriteLine("Decompression completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred during decompression: {ex.Message}");
        }
        stopwatch.Stop();
        Console.WriteLine($"耗时{stopwatch.Elapsed}秒");
        Console.ReadKey();
    }

    private static async Task DecompressFolderAsync(string sourceZipFile, string destinationDir, string dbFile)
    {
        using (var connection = new SqliteConnection($"Data Source={dbFile}"))
        {
            await connection.OpenAsync();

            using (var archive = ZipFile.OpenRead(sourceZipFile))
            {
                int entryCount = archive.Entries.Count;
                int currentEntry = 0;

                foreach (var entry in archive.Entries)
                {
                    currentEntry++;
                    string destinationPath = Path.GetFullPath(Path.Combine(destinationDir, entry.FullName));

                    if (!destinationPath.StartsWith(destinationDir, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"Skipping entry {entry.FullName} as it's outside the destination directory.");
                        continue;
                    }

                    if (string.IsNullOrEmpty(entry.Name))
                    {
                        Directory.CreateDirectory(destinationPath);
                    }
                    else
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath));

                        if (!await ExtractFileWithRetryAsync(entry, destinationPath))
                        {
                            _pendingFiles.Add((entry.FullName, destinationPath));
                        }
                        else
                        {
                            await RestoreFileMetadataAsync(connection, entry.FullName, destinationPath);
                        }

                        await HandleDuplicateFilesAsync(connection, entry.FullName, destinationDir);
                    }

                    Console.WriteLine($"Decompressed {currentEntry} of {entryCount}: {entry.FullName}");
                }
            }
        }
    }

    private static async Task<bool> ExtractFileWithRetryAsync(ZipArchiveEntry entry, string destinationPath)
    {
        int maxRetries = 5;
        int retryDelay = 1000; // 1 second

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                entry.ExtractToFile(destinationPath, true);
                return true; // Successful extraction
            }
            catch (IOException ex) when (IsFileLocked(ex))
            {
                if (attempt == maxRetries - 1)
                {
                    Console.WriteLine($"Unable to access file {destinationPath}, will retry later.");
                    return false; // Failed after all retries
                }
                await Task.Delay(retryDelay);
            }
        }
        return false; // This line should never be reached, but is needed for compilation
    }

    private static async Task RestoreFileMetadataAsync(SqliteConnection connection, string filePath, string destinationPath)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                SELECT CreationTime, LastWriteTime, LastAccessTime, Attributes
                FROM FileMetadata
                WHERE FilePath = @path";
            command.Parameters.AddWithValue("@path", filePath);

            using (var reader = await command.ExecuteReaderAsync())
            {
                if (await reader.ReadAsync())
                {
                    DateTime creationTime = DateTime.Parse(reader.GetString(0));
                    DateTime lastWriteTime = DateTime.Parse(reader.GetString(1));
                    DateTime lastAccessTime = DateTime.Parse(reader.GetString(2));
                    FileAttributes attributes = (FileAttributes)Enum.Parse(typeof(FileAttributes), reader.GetString(3));

                    File.SetCreationTime(destinationPath, creationTime);
                    File.SetLastWriteTime(destinationPath, lastWriteTime);
                    File.SetLastAccessTime(destinationPath, lastAccessTime);
                    File.SetAttributes(destinationPath, attributes);
                }
            }
        }
    }

    private static async Task HandleDuplicateFilesAsync(SqliteConnection connection, string originalPath, string destinationDir)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT DuplicatePath FROM DuplicateFiles WHERE OriginalPath = @original";
            command.Parameters.AddWithValue("@original", originalPath);

            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    string duplicatePath = reader.GetString(0);
                    string duplicateDestinationPath = Path.GetFullPath(Path.Combine(destinationDir, duplicatePath));
                    Directory.CreateDirectory(Path.GetDirectoryName(duplicateDestinationPath));

                    string sourcePath = Path.Combine(destinationDir, originalPath);
                    if (sourcePath.Equals(duplicateDestinationPath, StringComparison.OrdinalIgnoreCase))
                    {
                        continue; // Skip if source and destination are the same
                    }
                    if (!await CopyFileWithRetryAsync(sourcePath, duplicateDestinationPath))
                    {
                        _pendingFiles.Add((sourcePath, duplicateDestinationPath));
                    }
                    else
                    {
                        await RestoreFileMetadataAsync(connection, duplicatePath, duplicateDestinationPath);
                    }
                }
            }
        }
    }

    private static async Task<bool> CopyFileWithRetryAsync(string sourcePath, string destinationPath)
    {
        int maxRetries = 5;
        int retryDelay = 1000; // 1 second

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                File.Copy(sourcePath, destinationPath, true);
                return true; // Successful copy
            }
            catch (IOException ex) when (IsFileLocked(ex))
            {
                if (attempt == maxRetries - 1)
                {
                    Console.WriteLine($"Unable to copy file {sourcePath} to {destinationPath}, will retry later.");
                    return false; // Failed after all retries
                }
                await Task.Delay(retryDelay);
            }
        }
        return false; // This line should never be reached, but is needed for compilation
    }

    private static bool IsFileLocked(IOException ex)
    {
        int errorCode = System.Runtime.InteropServices.Marshal.GetHRForException(ex) & ((1 << 16) - 1);
        return errorCode == 32 || errorCode == 33;
    }

    private static async Task ProcessPendingFilesAsync(string dbFile)
    {
        if (_pendingFiles.Count == 0)
        {
            return;
        }

        Console.WriteLine("Processing files that couldn't be decompressed earlier...");

        using (var connection = new SqliteConnection($"Data Source={dbFile}"))
        {
            await connection.OpenAsync();

            foreach (var (source, destination) in _pendingFiles)
            {
                try
                {
                    File.Copy(source, destination, true);
                    await RestoreFileMetadataAsync(connection, source, destination);
                    Console.WriteLine($"Successfully processed file: {destination}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to process file {destination}: {ex.Message}");
                }
            }
        }

        await Task.Delay(5000); // Wait for 5 seconds to allow the file system to complete operations

        _pendingFiles.Clear();
    }
}
class ProgramZip
{
    private const string COMPRESSED_FILE_NAME = "compressed.zip";
    private const string DB_FILE_NAME = "file_info.db";
    private const string SQLITE_DLL = "e_sqlite3.dll";
    private const int BUFFER_SIZE = 4096;
    private const long SMALL_FILE_THRESHOLD = 100 * 1024; // 100 KB
    private const int PARTIAL_HASH_SIZE = 30 * 1024; // 30 KB

    internal static async Task Main(string[] args)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        string currentDirectory = Directory.GetCurrentDirectory();
        string compressedFilePath = Path.Combine(currentDirectory, COMPRESSED_FILE_NAME);
        string dbFilePath = Path.Combine(currentDirectory, DB_FILE_NAME);

        Console.WriteLine("Starting compression of the current directory...");

        try
        {
            if (File.Exists(compressedFilePath))
            {
                File.Delete(compressedFilePath);
                Console.WriteLine($"Deleted existing {COMPRESSED_FILE_NAME}");
            }

            if (File.Exists(dbFilePath))
            {
                File.Delete(dbFilePath);
                Console.WriteLine($"Deleted existing {DB_FILE_NAME}");
            }

            await CompressFolderAsync(currentDirectory, compressedFilePath, dbFilePath);
            Console.WriteLine("Compression completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred during compression: {ex.Message}");
        }
        stopwatch.Stop();
        Console.WriteLine($"耗时{stopwatch.Elapsed}秒");
        Console.ReadKey();
    }

    private static async Task CompressFolderAsync(string sourceDir, string destZipFile, string dbFile)
    {
        using (var connection = new SqliteConnection($"Data Source={dbFile}"))
        {
            connection.Open();

            CreateTables(connection);

            var filesToCompress = await GetFilesForCompressionAsync(sourceDir, connection);

            using (var zipStream = new FileStream(destZipFile, FileMode.Create))
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true))
            {
                int fileCount = 0;

                foreach (var kvp in filesToCompress)
                {
                    var originalPath = kvp.Key;
                    var uniquePath = kvp.Value;
                    var entryName = Path.GetRelativePath(sourceDir, uniquePath);

                    if (originalPath == uniquePath)
                    {
                        var fileInfo = new FileInfo(uniquePath);
                        var entry = archive.CreateEntry(entryName, CompressionLevel.Optimal);

                        // Set the original LastWriteTime
                        entry.LastWriteTime = fileInfo.LastWriteTime;

                        using (var entryStream = entry.Open())
                        using (var fileStream = File.OpenRead(uniquePath))
                        {
                            await fileStream.CopyToAsync(entryStream);
                        }

                        // Store original file metadata
                        StoreOriginalMetadata(connection, entryName, fileInfo);

                        fileCount++;
                        Console.WriteLine($"Compressed file {fileCount} of {filesToCompress.Count}: {entryName}");
                    }

                    // Store deduplication info in the database
                    StoreDuplicateInfo(connection, entryName, Path.GetRelativePath(sourceDir, originalPath));
                }
            }
        }
    }

    private static void CreateTables(SqliteConnection connection)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                CREATE TABLE IF NOT EXISTS FileHashes (
                    FilePath TEXT PRIMARY KEY,
                    MetadataHash TEXT NOT NULL,
                    ContentHash TEXT NOT NULL
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
                    Attributes TEXT NOT NULL,
                    Length INTEGER NOT NULL
                );";
            command.ExecuteNonQuery();
        }
    }

    private static async Task<Dictionary<string, string>> GetFilesForCompressionAsync(string sourceDir, SqliteConnection connection)
    {
        var allFiles = Directory.GetFiles(sourceDir, "*.*", SearchOption.AllDirectories);
        var fileMapping = new Dictionary<string, string>();

        foreach (var file in allFiles)
        {
            if (ShouldSkipFile(file)) continue;

            var fileInfo = new FileInfo(file);
            var metadataHash = CalculateMetadataHash(fileInfo);
            var contentHash = await CalculateFileHashAsync(file);

            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT FilePath FROM FileHashes WHERE MetadataHash = @metadataHash AND ContentHash = @contentHash";
                command.Parameters.AddWithValue("@metadataHash", metadataHash);
                command.Parameters.AddWithValue("@contentHash", contentHash);
                var result = command.ExecuteScalar();

                if (result == null)
                {
                    // New unique file
                    command.CommandText = "INSERT INTO FileHashes (FilePath, MetadataHash, ContentHash) VALUES (@path, @metadataHash, @contentHash)";
                    command.Parameters.AddWithValue("@path", file);
                    command.ExecuteNonQuery();
                    fileMapping[file] = file;
                }
                else
                {
                    // Duplicate file
                    fileMapping[file] = result.ToString();
                }
            }
        }

        return fileMapping;
    }

    private static string CalculateMetadataHash(FileInfo fileInfo)
    {
        var metadata = $"{fileInfo.LastWriteTimeUtc}|{fileInfo.Attributes}|{fileInfo.Length}";
        using (var md5 = MD5.Create())
        {
            var hashBytes = md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes(metadata));
            return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
        }
    }

    private static bool ShouldSkipFile(string filePath)
    {
        string fileName = Path.GetFileName(filePath);
        string fileDirectory = Path.GetDirectoryName(filePath);
        string _executingDirectory = Path.GetDirectoryName(Environment.ProcessPath);

        // 检查是否是程序运行文件夹中的 e_sqlite3.dll
        if (fileName.Equals("e_sqlite3.dll", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(fileDirectory, _executingDirectory, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return fileName.StartsWith("notZip", StringComparison.OrdinalIgnoreCase) ||
               fileName.Equals(COMPRESSED_FILE_NAME, StringComparison.OrdinalIgnoreCase) ||
               fileName.Equals(DB_FILE_NAME, StringComparison.OrdinalIgnoreCase)
               ;
    }

    private static void StoreDuplicateInfo(SqliteConnection connection, string originalPath, string duplicatePath)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "INSERT OR IGNORE INTO DuplicateFiles (OriginalPath, DuplicatePath) VALUES (@original, @duplicate)";
            command.Parameters.AddWithValue("@original", originalPath);
            command.Parameters.AddWithValue("@duplicate", duplicatePath);
            command.ExecuteNonQuery();
        }
    }

    private static void StoreOriginalMetadata(SqliteConnection connection, string filePath, FileInfo fileInfo)
    {
        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                INSERT OR REPLACE INTO FileMetadata (FilePath, CreationTime, LastWriteTime, LastAccessTime, Attributes, Length)
                VALUES (@path, @creationTime, @lastWriteTime, @lastAccessTime, @attributes, @length)";
            command.Parameters.AddWithValue("@path", filePath);
            command.Parameters.AddWithValue("@creationTime", fileInfo.CreationTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@lastWriteTime", fileInfo.LastWriteTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@lastAccessTime", fileInfo.LastAccessTimeUtc.ToString("o"));
            command.Parameters.AddWithValue("@attributes", fileInfo.Attributes.ToString());
            command.Parameters.AddWithValue("@length", fileInfo.Length);
            command.ExecuteNonQuery();
        }
    }

    private static async Task<string> CalculateFileHashAsync(string filePath)
    {
        var fileInfo = new FileInfo(filePath);

        if (fileInfo.Length < SMALL_FILE_THRESHOLD)
        {
            return await CalculateFullFileHashAsync(filePath);
        }
        else
        {
            return await CalculatePartialFileHashAsync(filePath);
        }
    }

    private static async Task<string> CalculateFullFileHashAsync(string filePath)
    {
        using (var md5 = MD5.Create())
        using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, BUFFER_SIZE, true))
        {
            var hash = await md5.ComputeHashAsync(stream);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }
    }

    private static async Task<string> CalculatePartialFileHashAsync(string filePath)
    {
        using (var md5 = MD5.Create())
        using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, BUFFER_SIZE, true))
        {
            var fileLength = stream.Length;
            var buffer = new byte[PARTIAL_HASH_SIZE];

            // Read beginning
            await stream.ReadAsync(buffer, 0, PARTIAL_HASH_SIZE);
            md5.TransformBlock(buffer, 0, PARTIAL_HASH_SIZE, null, 0);

            // Read middle
            stream.Position = (fileLength - PARTIAL_HASH_SIZE) / 2;
            await stream.ReadAsync(buffer, 0, PARTIAL_HASH_SIZE);
            md5.TransformBlock(buffer, 0, PARTIAL_HASH_SIZE, null, 0);

            // Read end
            stream.Position = Math.Max(0, fileLength - PARTIAL_HASH_SIZE);
            int bytesRead = await stream.ReadAsync(buffer, 0, PARTIAL_HASH_SIZE);
            md5.TransformFinalBlock(buffer, 0, bytesRead);

            var hash = md5.Hash;
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }
    }
}