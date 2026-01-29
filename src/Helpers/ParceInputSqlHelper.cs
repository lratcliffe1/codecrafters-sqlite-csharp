namespace codecrafters_sqlite.src.Helpers;

public class ParsedInput()
{
  public required string BaseCommand { get; set; }
  public required List<string> Selected { get; set; }
  public required string TableName { get; set; }

  public required Func<Dictionary<string, string>, bool> Conditional { get; set; }
}

public static class ParceInputSqlHelper
{
  public static ParsedInput ParseInput(string command)
  {
    var splitCommand = command
      .Replace(",", "")
      .Replace("from", "FROM")
      .Replace("where", "WHERE")
      .Split(" ")
      .ToList();

    int fromIndex = splitCommand.IndexOf("FROM");
    int whereIndex = splitCommand.IndexOf("WHERE");

    Func<Dictionary<string, string>, bool> conditional = static _ => true;

    if (whereIndex != -1)
    {
      string columnToCompare = splitCommand[whereIndex + 1];
      string valueToCompare = splitCommand.Last().Replace("\'", "");

      conditional = (keyValuePair) => keyValuePair[columnToCompare] == valueToCompare;
    }

    return new ParsedInput()
    {
      BaseCommand = splitCommand.First(),
      Selected = splitCommand[1..fromIndex],
      TableName = splitCommand[fromIndex + 1],
      Conditional = conditional
    };
  }
}