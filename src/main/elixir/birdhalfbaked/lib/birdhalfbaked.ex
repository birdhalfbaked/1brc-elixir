defmodule Birdhalfbaked do
  @moduledoc """
  Documentation for `Birdhalfbaked`.
  """

  defp convertRow(row) do
    [k, v] = String.split(String.trim(row), ";")
    {k, String.to_float(v)}
  end

  defp updateMap(data, map) do
    # {num, sum, min, max}
    el = elem(data, 1)

    Map.update(map, elem(data, 0), {1, el, el, el}, fn {num, sum, min, max} ->
      {nmin, nmax} =
        cond do
          el >= max ->
            {min, el}

          el <= min ->
            {el, max}

          true ->
            {min, max}
        end

      {num + 1, sum + el, nmin, nmax}
    end)
  end

  defp mergeMap(map1, map2) do
    Map.merge(map1, map2, fn _, {num1, sum1, min1, max1}, {num2, sum2, min2, max2} ->
      nmin =
        if min1 < min2 do
          min1
        else
          min2
        end

      nmax =
        if max1 > max2 do
          max1
        else
          max2
        end

      {num1 + num2, sum1 + sum2, nmin, nmax}
    end)
  end

  defp reader(filepath, chunkSize, pos, parent) do
    file = File.open!(filepath, [:read])
    {:ok, data} = :file.pread(file, pos * chunkSize, chunkSize)

    # leave the \r for some cheeky hack later to split again :)
    [first_line | splits] = String.split(data, "\n")
    [last_line | splits] = Enum.reverse(splits)

    map =
      splits
      |> Enum.reduce(
        %{},
        fn v, acc ->
          updateMap(convertRow(v), acc)
        end
      )

    File.close(file)
    send(parent, {pos, {first_line, map, last_line}})
  end

  defp doPartRead(filepath, chunkSize, maxWorkers, setNum) do
    self = self()
    resultArr = Enum.to_list(1..maxWorkers)

    data =
      1..maxWorkers
      |> Enum.map(fn i ->
        spawn(fn -> reader(filepath, chunkSize, i - 1 + maxWorkers * setNum, self) end)
      end)
      |> Enum.reduce(resultArr, fn _, acc ->
        {i, response_tuple} =
          receive do
            data ->
              IO.puts("#{elem(data, 0)} done")
              data
          end

        List.replace_at(acc, rem(i, maxWorkers), response_tuple)
      end)

    ret =
      data
      |> Enum.reduce({%{}, ""}, fn {first, datamap, last}, {mp, remainder} ->
        {mergeMap(mp, datamap), remainder <> first <> last}
      end)

    ret
  end

  defp multireader(filepath, numWorkers, maxConcurrentWorkers) do
    {:ok, stat} = File.stat(filepath)
    chunkSize = ceil(stat.size / numWorkers)

    # resultArr = Enum.to_list(0..numWorkers)

    workerChunks = ceil(numWorkers / maxConcurrentWorkers)

    {data_map, remainders} =
      1..workerChunks
      |> Enum.map(fn i -> doPartRead(filepath, chunkSize, maxConcurrentWorkers, i - 1) end)
      |> Enum.reduce({%{}, ""}, fn {map, remainder}, {cur_map, cur_remainder} ->
        {mergeMap(map, cur_map), cur_remainder <> remainder}
      end)

    # now we split on \r
    final_map =
      String.split(remainders, "\r")
      |> Enum.filter(fn v -> v != "" end)
      |> Enum.reduce(
        %{},
        fn v, acc ->
          updateMap(convertRow(v), acc)
        end
      )

    ret =
      mergeMap(final_map, data_map)
      |> Enum.map(fn {k, {num, sum, min, max}} ->
        "#{k}=#{Float.round(sum / num, 1)}/#{Float.round(min, 1)}/#{Float.round(max, 1)}"
      end)
      |> Enum.sort()
      |> Enum.join(", ")

    ret

    # IO.inspect(all_data)
    # data =
    #   1..numWorkers
    #   |> Enum.map(fn i ->
    #     spawn(fn -> reader(filepath, chunkSize, i - 1, self) end)
    #   end)
    #   |> Enum.reduce(resultArr, fn _, acc ->
    #     {i, response_tuple} =
    #       receive do
    #         data ->
    #           IO.puts("#{elem(data, 0)} done")
    #           data
    #       end

    #     List.replace_at(acc, i, response_tuple)
    #   end)

    # map = data |> Enum.reduce(%{}, fn v, map -> updateMap(elem(v, 1), map) end)

    # remainders =
    #   Enum.with_index(data)
    #   |> Enum.filter(fn {v, i} -> i > 0 end)
    #   |> Enum.reduce([], fn {v, i}, acc ->
    #     [acc | elem(Enum.at(v, i - 1), 2) <> elem(Enum.at(v, i), 0)]
    #   end)

    # map = remainders |> Enum.reduce(map, fn v, map -> updateMap(elem(v, 1), map) end)

    # Map.to_list(map) |> Enum.map(fn {k, {n, v}} -> {k, v / n} end)
  end

  def read_lines(filepath, numWorkers, maxConcurrentWorkers) do
    :timer.tc(&multireader/3, [filepath, numWorkers, maxConcurrentWorkers])
  end
end
