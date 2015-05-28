defmodule MongodbEcto.Bson do
  # FIXME the ObjectId -> integer hack

  def from_bson(document, pk \\ :id) do
    document
    |> Tuple.to_list
    |> Enum.chunk(2)
    |> Enum.into(%{}, fn
      [:_id, {<<value :: integer-size(96)>>}] -> {pk, value}
      [key, value] -> {key, decode(value)}
    end)
  end

  defp decode(:null), do: nil
  defp decode({msec, sec, usec} = datetime)
    when is_integer(msec) and is_integer(sec) and is_integer(usec) do
    {date, {hour, minute, second}} =
      :calendar.now_to_universal_time(datetime)
    {date, {hour, minute, second, usec}}
  end
  defp decode(document) when is_tuple(document), do: from_bson(document)
  defp decode(other), do: other

  def to_bson(document, pk \\ :id) do
    document
    |> Enum.flat_map(fn
      {key, value} when key == pk -> [:_id, {<<value :: integer-size(96)>>}]
      {key, value} -> [key, encode(value)]
    end)
    |> List.to_tuple
  end

  defp encode(nil), do: :null
  defp encode({{year, month, day}, {hour, min, sec, usec}}) do
    seconds =
      :calendar.datetime_to_gregorian_seconds({{year, month, day}, {hour, min, sec}})
    {div(seconds, 1000000), rem(seconds, 1000000), usec}
  end
  defp encode(%Ecto.Query.Tagged{value: value}), do: value
  defp encode(other), do: other
end
