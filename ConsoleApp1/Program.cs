using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using System.Diagnostics;

class Program
{
    private const string COMPRESSED_FILE_NAME = "compressed.zip";
    private const string DB_FILE_NAME = "file_info.db";
    private static List<(string source, string destination)> _pendingFiles = new List<(string, string)>();

    static async Task Main(string[] args)
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