namespace codecrafters_sqlite.src.Helpers;

public static class PageHelper
{
  public static long GetPageStart(uint pageSize, int pageNumber)
  {
    return (pageNumber - 1) * pageSize;
  }
}
