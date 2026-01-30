using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class DbInfo
{
  public static void Process(Stream databaseFile)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader schemaHeader = HeaderHelper.ReadPageHeader(databaseFile, SqliteConstants.SchemaPageNumber, databaseHeader.PageSize);

    Console.WriteLine($"database page size: {databaseHeader.PageSize}");
    Console.WriteLine($"number of tables: {schemaHeader.CellCount}");

    // Helper.SeeData(databaseHeader);
    // Console.WriteLine("");
    // Helper.SeeData(bTreeHeader);
  }
}