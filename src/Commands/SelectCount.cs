using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class SelectCount
{
  public static void Process(FileStream databaseFile, string command)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader bTreeHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

    List<int> cellPointerArray = CellPointerHelper.GetCellPointerArray(databaseFile, bTreeHeader.PageType, 1, databaseHeader.PageSize, bTreeHeader.CellCount);
    List<Record> records = RecordHelper.GetRecordData(databaseFile, cellPointerArray);

    string tableName = command.Substring("SELECT COUNT(*) FROM ".Length).Trim('\"', ';', ' ');
    int page = records.First(x => x.Name == tableName).RootPage;

    BTreePageHeader bTreePageHeader = HeaderHelper.ReadPageHeader(databaseFile, page, databaseHeader.PageSize);

    Console.WriteLine(bTreePageHeader.CellCount);
  }
}