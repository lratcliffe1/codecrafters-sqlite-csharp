namespace codecrafters_sqlite.src.Helpers;

public class ParsedInput
{
  public required string BaseCommand { get; set; }
  public required List<string> Selected { get; set; }
  public required string TableName { get; set; }

  public required Func<Dictionary<string, string>, bool> Conditional { get; set; }
  public string? WhereColumn { get; set; }
  public string? WhereValue { get; set; }
}

public class ParsedIndexSQL
{
  public required string OnTable { get; set; }
  public required string OnColumn { get; set; }
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
    string? whereColumn = null;
    string? whereValue = null;

    if (whereIndex != -1)
    {
      string columnToCompare = splitCommand[whereIndex + 1];
      int equalsIndex = splitCommand.IndexOf("=");

      string valueToCompare = string.Join(
          " ",
          splitCommand[(equalsIndex + 1)..]
      ).Trim('\'');

      conditional = (keyValuePair) => keyValuePair[columnToCompare] == valueToCompare;
      whereColumn = columnToCompare;
      whereValue = valueToCompare;
    }

    return new ParsedInput()
    {
      BaseCommand = splitCommand.First(),
      Selected = splitCommand[1..fromIndex],
      TableName = splitCommand[fromIndex + 1],
      Conditional = conditional,
      WhereColumn = whereColumn,
      WhereValue = whereValue
    };
  }

  public static ParsedIndexSQL ParseIndexSQL(string command)
  {
    command = string.Join(
        " ",
        command.Split((char[])null!, StringSplitOptions.RemoveEmptyEntries)
    );

    var lower = command.ToLowerInvariant();

    int onIndex = lower.IndexOf(" on ");
    if (onIndex == -1)
      throw new ArgumentException("Invalid index SQL: missing ON");

    string afterOn = command[(onIndex + 4)..].Trim(); // after " on "

    int openParen = afterOn.IndexOf('(');
    int closeParen = afterOn.IndexOf(')', openParen + 1);

    if (openParen == -1 || closeParen == -1)
      throw new ArgumentException("Invalid index SQL: missing (column)");

    string table = afterOn[..openParen].Trim();
    string column = afterOn[(openParen + 1)..closeParen].Trim();

    if (column.Contains(","))
      throw new NotSupportedException("Only single-column indexes are supported");

    return new ParsedIndexSQL
    {
      OnTable = table,
      OnColumn = column
    };
  }
}