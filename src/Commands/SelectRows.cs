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

    RecursivelyCheckPointers(databaseFile, databaseHeader, bTreePageHeader, table, parsedInput, table.RootPage);
  }

  private static void RecursivelyCheckPointers(FileStream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader bTreePageHeader, TableData table, ParsedInput parsedInput, int pageNumber)
  {
    if (bTreePageHeader.PageType == 13)
    {
      List<int> cellPointerArray = CellPointerHelper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, pageNumber, databaseHeader.PageSize, bTreePageHeader.CellCount);

      RecordHelper.PrintRecordValues(databaseFile, databaseHeader, table, cellPointerArray, parsedInput, pageNumber);
    }
    else
    {
      List<int> cellPointerArray = CellPointerHelper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, pageNumber, databaseHeader.PageSize, bTreePageHeader.CellCount);

      long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

      foreach (var pointer in cellPointerArray)
      {
        databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);
        byte[] childBuffer = new byte[4];
        databaseFile.ReadExactly(childBuffer);
        int childPage = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(childBuffer);

        BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
        RecursivelyCheckPointers(databaseFile, databaseHeader, childHeader, table, parsedInput, childPage);
      }

      if (bTreePageHeader.RightMostPointer.HasValue)
      {
        int rightMostPage = (int)bTreePageHeader.RightMostPointer.Value;

        BTreePageHeader rightHeader = HeaderHelper.ReadPageHeader(databaseFile, rightMostPage, databaseHeader.PageSize);
        RecursivelyCheckPointers(databaseFile, databaseHeader, rightHeader, table, parsedInput, rightMostPage);
      }
    }
  }
}
