defmodule Mongo.Ecto.Bson do

  def from_bson(document, pk) do
    document
    |> Tuple.to_list
    |> Enum.chunk(2)
    |> Enum.into(%{}, fn
      [:_id, value] -> {pk, decode(value, pk)}
      [key, value] -> {key, decode(value, pk)}
    end)
  end

  defp decode(:null, _), do: nil
  # defp decode(:undefined, _) do
  #   raise ArgumentError, ":undefined is not a valid value"
  # end
  defp decode({objectid}, _) when is_binary(objectid), do: objectid
  defp decode({:bin, :uuid, uuid}, _), do: uuid
  defp decode({msec, sec, usec} = datetime, _)
    when is_integer(msec) and is_integer(sec) and is_integer(usec) do
    {date, {hour, minute, second}} =
      :calendar.now_to_universal_time(datetime)
    {date, {hour, minute, second, usec}}
  end
  defp decode(document, pk) when is_tuple(document), do: from_bson(document, pk)
  defp decode(other, _), do: other

  def to_bson(document, pk) do
    document
    |> Enum.flat_map(fn
      {key, value} when key == pk -> [:_id, encode(value, pk)]
      {key, value} -> [key, encode(value, pk)]
    end)
    |> List.to_tuple
  end

  @epoch_seconds :calendar.datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})

  defp encode(nil, _), do: :null
  defp encode({date, {hour, min, sec, usec}}, _) do
    datetime = {date, {hour, min, sec}}
    seconds = :calendar.datetime_to_gregorian_seconds(datetime) - @epoch_seconds
    {div(seconds, 1000000), rem(seconds, 1000000), usec}
  end
  defp encode(%Ecto.Query.Tagged{value: nil}, _), do: :null
  defp encode(%Ecto.Query.Tagged{type: :uuid, value: value}, _) do
    {:bin, :uuid, value}
  end
  defp encode(%Ecto.Query.Tagged{type: :object_id, value: value}, _) do
    {value}
  end
  defp encode(%Ecto.Query.Tagged{tag: nil, value: value}, _), do: value
  defp encode(%Ecto.Query.Tagged{} = value, _) do
    raise ArgumentError, "#{inspect value} is not a supported BSON type"
  end
  defp encode(%{__struct__: _} = struct, _) do
    raise ArgumentError, "#{inspect struct} is not a supported BSON type"
  end
  defp encode(map, pk) when is_map(map), do: to_bson(map, pk)
  defp encode(list, pk) when is_list(list), do: Enum.map(list, &encode(&1, pk))
  defp encode(other, _), do: other
end
