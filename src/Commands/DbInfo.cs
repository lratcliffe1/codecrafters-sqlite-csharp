using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class DbInfo
{
  public static void Process(FileStream databaseFile)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader bTreeHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

    Console.WriteLine($"database page size: {databaseHeader.PageSize}");
    Console.WriteLine($"number of tables: {bTreeHeader.CellCount}");
  }
}