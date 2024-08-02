using System;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Diagnostics;

class Program
{
    private const string COMPRESSED_FILE_NAME = "compressed.zip";
    private const string DB_FILE_NAME = "file_info.db";
    private const int BUFFER_SIZE = 4096;
    private const long SMALL_FILE_THRESHOLD = 100 * 1024; // 100 KB
    private const int PARTIAL_HASH_SIZE = 30 * 1024; // 30 KB

    static async Task Main(string[] args)
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
        return fileName.StartsWith("notZip", StringComparison.OrdinalIgnoreCase) ||
               fileName.Equals(COMPRESSED_FILE_NAME, StringComparison.OrdinalIgnoreCase) ||
               fileName.Equals(DB_FILE_NAME, StringComparison.OrdinalIgnoreCase);
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