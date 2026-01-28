
namespace codecrafters_sqlite.src.Classes;

public class BTreePageHeader
{
  public byte PageType { get; set; } // interior index b-tree or interior table b-tree or leaf index b-tree or leaf table b-tree
  public ushort FirstFreeblock { get; set; }
  public ushort CellCount { get; set; }
  public uint CellContentStart { get; set; } // uint to handle the 65536 case
  public byte FragmentedFreeBytes { get; set; }
  public uint? RightMostPointer { get; set; } // Nullable because it's optional
}
