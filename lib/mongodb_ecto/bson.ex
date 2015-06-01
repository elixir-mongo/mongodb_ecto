defmodule MongodbEcto.Bson do

  def from_bson(document, pk \\ :id) do
    document
    |> Tuple.to_list
    |> Enum.chunk(2)
    |> Enum.into(%{}, fn
      [:_id, value] -> {pk, decode(value)}
      [key, value] -> {key, decode(value)}
    end)
  end

  defp decode(:null), do: nil
  defp decode({objectid}) when is_binary(objectid), do: objectid
  defp decode({:bin, :uuid, uuid}), do: uuid
  defp decode({msec, sec, usec} = datetime)
    when is_integer(msec) and is_integer(sec) and is_integer(usec) do
    {date, {hour, minute, second}} =
      :calendar.now_to_universal_time(datetime)
    {date, {hour, minute, second, usec}}
  end
  defp decode(document) when is_tuple(document), do: from_bson(document)
  defp decode(other), do: other

  # FIXME we need some way to tell apart regular binary from objectid to
  #       encode it properely when in complex requests
  def to_bson(document, pk \\ :id) do
    document
    |> Enum.flat_map(fn
      {key, value} when key == pk and is_binary(value) -> [:_id, {value}]
      {key, value} when key == pk -> [:_id, encode(value)]
      {key, value} -> [key, encode(value)]
    end)
    |> List.to_tuple
  end

  @epoch_seconds :calendar.datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})

  defp encode(nil), do: :null
  defp encode({date, {hour, min, sec, usec}}) do
    datetime = {date, {hour, min, sec}}
    seconds = :calendar.datetime_to_gregorian_seconds(datetime) - @epoch_seconds
    {div(seconds, 1000000), rem(seconds, 1000000), usec}
  end
  defp encode(%Ecto.Query.Tagged{type: :uuid, value: value}) do
    {:bin, :uuid, value}
  end
  defp encode(%Ecto.Query.Tagged{type: :object_id, value: value}) do
    {value}
  end
  defp encode(%Ecto.Query.Tagged{tag: nil, value: value}), do: value
  defp encode(%Ecto.Query.Tagged{} = value) do
    raise ArgumentError, value
  end
  defp encode(map) when is_map(map), do: to_bson(map)
  defp encode(list) when is_list(list), do: Enum.map(list, &encode/1)
  defp encode(other), do: other
end
