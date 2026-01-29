namespace codecrafters_sqlite.src;

public static class Helper
{
  public static void SeeData<T>(T obj)
  {
    foreach (var prop in typeof(T).GetProperties())
    {
      string name = prop.Name;
      object? value = prop.GetValue(obj);

      Console.WriteLine($"{name,-25}: {value}");
    }
  }
}