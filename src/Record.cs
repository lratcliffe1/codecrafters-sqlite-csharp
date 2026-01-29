namespace codecrafters_sqlite.src;

public class Record
{
  public required string Type { get; set; }
  public required string Name { get; set; }
  public required string TableName { get; set; }
  public required int RootPage { get; set; }
  public required string SQL { get; set; }
}