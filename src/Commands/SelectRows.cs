using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class SelectRows
{
  public static void Process(FileStream databaseFile, string command)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader bTreeHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

    ParsedInput parsedInput = ParceInputSqlHelper.ParseInput(command);

    List<int> cellPointerArray = CellPointerHelper.GetCellPointerArray(databaseFile, bTreeHeader.PageType, 1, databaseHeader.PageSize, bTreeHeader.CellCount);
    List<Record> tables = RecordHelper.GetRecordData(databaseFile, cellPointerArray);

    Record record = tables.First(x => x.Name == parsedInput.TableName);
    TableData table = RecordHelper.ParceTableData(record);

    BTreePageHeader bTreePageHeader = HeaderHelper.ReadPageHeader(databaseFile, table.RootPage, databaseHeader.PageSize);
    cellPointerArray = CellPointerHelper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, table.RootPage, databaseHeader.PageSize, bTreePageHeader.CellCount);

    RecordHelper.PrintRecordValues(databaseFile, databaseHeader, table, cellPointerArray, parsedInput);
  }
}
