defmodule Birdhalfbaked do
  @moduledoc """
  Documentation for `Birdhalfbaked`.
  """

  defp convertRow(row) do
    String.split(row, ";")
    |> then(fn v -> {List.first(v), Float.parse(List.last(v)) |> elem(0)} end)
  end

  defp updateMap(data, map) do
    Map.update(map, elem(data, 0), {1, elem(data, 1)}, fn v ->
      {elem(v, 0) + 1, elem(v, 1) + elem(data, 1)}
    end)
  end

  defp reader(filepath, workerNum, parent) do
    file = File.stream!(filepath, :line, [])

    map =
      Stream.with_index(file)
      |> Stream.filter(fn {_, i} -> rem(i + 1, workerNum) == 0 end)
      |> Stream.map(fn {v, _} ->
        convertRow(v)
      end)
      |> Enum.reduce(%{}, fn el, acc -> updateMap(el, acc) end)

    result = Map.to_list(map) |> Enum.map(fn {k, {n, v}} -> {k, v / n} end)
    IO.inspect(workerNum)
    IO.inspect(Enum.count(result))
    send(parent, result)
  end

  defp multireader(filepath, numWorkers) do
    self = self()

    1..numWorkers
    |> Enum.map(fn i -> spawn(fn -> reader(filepath, i, self) end) end)
    |> Enum.each(fn _ ->
      receive do
        msg ->
          IO.inspect("done")
      end
    end)
  end

  @spec read_lines(any(), any()) :: {integer(), any()}
  def read_lines(filepath, numWorkers) do
    :timer.tc(&multireader/2, [filepath, numWorkers])
  end
end
