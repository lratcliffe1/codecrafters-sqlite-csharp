using codecrafters_sqlite.src;

var (path, command) = args.Length switch
{
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

FileStream? databaseFile = File.OpenRead(path);

DatabaseHeader databaseHeader = Helper.ReadDatabaseHeader(databaseFile);
BTreePageHeader bTreePageHeader = Helper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

if (command == ".dbinfo")
{
    databaseFile.Seek(0, SeekOrigin.Begin);

    Console.WriteLine($"database page size: {databaseHeader.PageSize}");

    Console.WriteLine($"number of tables: {bTreePageHeader.CellCount}");

    // Helper.SeeData(databaseHeader);
    // Helper.SeeData(bTreePageHeader);
}
else if (command == ".tables")
{
    List<int> cellPointers = Helper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, 1, databaseHeader.PageSize, bTreePageHeader.CellCount);

    foreach (var pointer in cellPointers)
    {
        var row = Helper.GetRecordData(databaseFile, pointer);

        Console.Write($"{row.Name} ");
    }
}
else
{
    throw new InvalidOperationException($"Invalid command: {command}");
}