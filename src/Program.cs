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
BTreePageHeader bTreeHeader = Helper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

if (command == ".dbinfo")
{
  Console.WriteLine($"database page size: {databaseHeader.PageSize}");
  Console.WriteLine($"number of tables: {bTreeHeader.CellCount}");
}
else if (command == ".tables")
{
  List<int> cellPointerArray = Helper.GetCellPointerArray(databaseFile, bTreeHeader.PageType, 1, databaseHeader.PageSize, bTreeHeader.CellCount);
  List<Record> records = Helper.GetRecordData(databaseFile, cellPointerArray);

  Console.WriteLine(string.Join(" ", records.Select(x => x.Name)));
}
else if (command.StartsWith("SELECT COUNT(*)", StringComparison.OrdinalIgnoreCase))
{
  List<int> cellPointerArray = Helper.GetCellPointerArray(databaseFile, bTreeHeader.PageType, 1, databaseHeader.PageSize, bTreeHeader.CellCount);
  List<Record> records = Helper.GetRecordData(databaseFile, cellPointerArray);

  string tableName = command.Substring("SELECT COUNT(*) FROM ".Length).Trim('\"', ';', ' ');
  int page = records.First(x => x.Name == tableName).RootPage;

  BTreePageHeader bTreePageHeader = Helper.ReadPageHeader(databaseFile, page, databaseHeader.PageSize);

  Console.WriteLine(bTreePageHeader.CellCount);
}
else
{
  throw new InvalidOperationException($"Invalid command: {command}");
}