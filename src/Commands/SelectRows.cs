using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class SelectRows
{
  private const int LeafTablePageType = 13;

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

    RecursivelyCheckPointers(databaseFile, databaseHeader, bTreePageHeader, table, parsedInput, table.RootPage);
  }

  private static void RecursivelyCheckPointers(FileStream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader bTreePageHeader, TableData table, ParsedInput parsedInput, int pageNumber)
  {
    List<int> cellPointers = CellPointerHelper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, pageNumber, databaseHeader.PageSize, bTreePageHeader.CellCount);

    if (bTreePageHeader.PageType == LeafTablePageType)
    {
      RecordHelper.PrintRecordValues(databaseFile, databaseHeader, table, cellPointers, parsedInput, pageNumber);
      return;
    }

    long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

    foreach (var childPage in GetChildPages(databaseFile, pageStart, cellPointers))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      RecursivelyCheckPointers(databaseFile, databaseHeader, childHeader, table, parsedInput, childPage);
    }
  }

  private static IEnumerable<int> GetChildPages(FileStream databaseFile, long pageStart, IEnumerable<int> cellPointers)
  {
    foreach (var pointer in cellPointers)
    {
      yield return ReadChildPageNumber(databaseFile, pageStart, pointer);
    }
  }

  private static int ReadChildPageNumber(FileStream databaseFile, long pageStart, int cellPointer)
  {
    databaseFile.Seek(pageStart + cellPointer, SeekOrigin.Begin);
    byte[] buffer = new byte[4];
    databaseFile.ReadExactly(buffer);
    return (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(buffer);
  }
}
