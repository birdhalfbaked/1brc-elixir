defmodule Main do
  def main() do
    IO.inspect(Birdhalfbaked.read_lines("../../../../measurements.txt", 15 * 20, 15))
  end
end

Main.main()
