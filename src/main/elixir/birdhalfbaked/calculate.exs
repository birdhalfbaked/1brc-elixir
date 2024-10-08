defmodule Main do
  def main() do
    IO.inspect(Birdhalfbaked.read_lines("../../../../measurements.txt", 16))
  end
end

Main.main()
