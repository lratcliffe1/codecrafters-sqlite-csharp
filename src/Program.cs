using codecrafters_sqlite.src;
using codecrafters_sqlite.src.Classes;

var (path, command) = args.Length switch
{
  0 => throw new InvalidOperationException("Missing <database path> and <command>"),
  1 => throw new InvalidOperationException("Missing <command>"),
  _ => (args[0], args[1])
};

FileStream? databaseFile = File.OpenRead(path);

DatabaseHeader databaseHeader = Helper.ReadDatabaseHeader(databaseFile);
BTreePageHeader bTreePageHeader = Helper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);
List<int> cellPointerArray = Helper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, 1, databaseHeader.PageSize, bTreePageHeader.CellCount);

if (command == ".dbinfo")
{
  Console.WriteLine($"database page size: {databaseHeader.PageSize}");
  Console.WriteLine($"number of tables: {bTreePageHeader.CellCount}");
}
else if (command == ".tables")
{
  List<Record> records = Helper.GetRecordData(databaseFile, cellPointerArray);
  Console.WriteLine(string.Join(" ", records.Select(x => x.Name)));
}
else
{
  throw new InvalidOperationException($"Invalid command: {command}");
}