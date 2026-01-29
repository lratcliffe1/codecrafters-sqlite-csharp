using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class SelectCount
{
  private const string SelectCountPrefix = "SELECT COUNT(*) FROM ";

  public static void Process(FileStream databaseFile, string command)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader schemaHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

    List<int> cellPointerArray = CellPointerHelper.ReadCellPointers(databaseFile, schemaHeader.PageType, 1, databaseHeader.PageSize, schemaHeader.CellCount);
    List<Record> schemaRecords = RecordHelper.ReadSchemaRecords(databaseFile, cellPointerArray);

    string tableName = command.Substring(SelectCountPrefix.Length).Trim('\"', ';', ' ');
    int page = schemaRecords.First(x => x.Name == tableName).RootPage;

    BTreePageHeader bTreePageHeader = HeaderHelper.ReadPageHeader(databaseFile, page, databaseHeader.PageSize);

    Console.WriteLine(bTreePageHeader.CellCount);
  }
}