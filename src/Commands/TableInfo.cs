using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class TableInfo
{
  public static void Process(FileStream databaseFile)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader schemaHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

    List<int> cellPointerArray = CellPointerHelper.ReadCellPointers(databaseFile, schemaHeader.PageType, 1, databaseHeader.PageSize, schemaHeader.CellCount);
    List<Record> records = RecordHelper.ReadSchemaRecords(databaseFile, cellPointerArray);

    Console.WriteLine(string.Join(" ", records.Select(x => x.Name)));

    // foreach (var r in records)
    //   Helper.SeeData(r);
  }
}