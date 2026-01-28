using codecrafters_sqlite.src;

var (path, command) = args.Length switch
{
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

FileStream? databaseFile = File.OpenRead(path);

if (command == ".dbinfo")
{
    databaseFile.Seek(0, SeekOrigin.Begin);

    DatabaseHeader databaseHeader = Helper.ReadDatabaseHeader(databaseFile);
    Console.WriteLine($"database page size: {databaseHeader.PageSize}");

    BTreePageHeader bTreePageHeader = Helper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);
    Console.WriteLine($"number of tables: {bTreePageHeader.CellCount}");

    // Helper.SeeData(databaseHeader);
    // Helper.SeeData(bTreePageHeader);
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}