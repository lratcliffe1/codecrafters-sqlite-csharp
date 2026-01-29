using System.Text;
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
else if (command.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase))
{
  var splitCommand = command.Split(" ").ToList();
  var output = splitCommand.First();
  var selected = splitCommand[1];
  var tableName = splitCommand.Last();

  List<int> cellPointerArray = Helper.GetCellPointerArray(databaseFile, bTreeHeader.PageType, 1, databaseHeader.PageSize, bTreeHeader.CellCount);
  List<Record> tables = Helper.GetRecordData(databaseFile, cellPointerArray);

  Record record = tables.First(x => x.Name == tableName);
  TableData table = Helper.ParceTableData(record);

  BTreePageHeader bTreePageHeader = Helper.ReadPageHeader(databaseFile, table.RootPage, databaseHeader.PageSize);
  cellPointerArray = Helper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, table.RootPage, databaseHeader.PageSize, bTreePageHeader.CellCount);

  var pageStart = databaseHeader.PageSize * (table.RootPage - 1);

  foreach (ushort pointer in cellPointerArray)
  {
    databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);

    var (payloadSize, _) = Helper.ReadVarint(databaseFile);
    var (rowId, _) = Helper.ReadVarint(databaseFile);

    // 1. Read the Record Header Size
    var (headerSize, headerSizeLen) = Helper.ReadVarint(databaseFile);

    // 2. Read all the Serial Type Varints in the header
    int bytesToRead = (int)headerSize - headerSizeLen;
    byte[] headerBuffer = new byte[bytesToRead];
    databaseFile.ReadExactly(headerBuffer);

    List<int> columnLengths = [];
    int offset = 0;
    while (offset < headerBuffer.Length)
    {
      ulong serialType = Helper.ReadVarint(headerBuffer, ref offset);
      columnLengths.Add(Helper.GetSerialTypeLength(serialType));
    }

    // 3. Read the columns from the body using the lengths
    int i = 0;

    foreach (var column in table.Columns)
    {
      byte[] dataBytes = new byte[columnLengths[i++]];
      databaseFile.ReadExactly(dataBytes);

      if (column.Name != selected)
        continue;

      Console.WriteLine(Encoding.UTF8.GetString(dataBytes));
    }
  }
}
else
{
  throw new InvalidOperationException($"Invalid command: {command}");
}