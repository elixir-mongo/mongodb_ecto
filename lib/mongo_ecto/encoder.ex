defmodule Mongo.Ecto.Encoder do
  @moduledoc false

  alias Ecto.Query.Tagged

  def encode_document(doc, pk) do
    Enum.into(doc, %{}, fn
      {key, value} when key == pk -> {:_id, encode_value(value)}
      {key, value}                -> {key , encode_value(value)}
    end)
  end
  def encode_document(doc, params, pk) do
    Enum.into(doc, %{}, fn
      {key, value} when key == pk -> {:_id, encode_value(value, params)}
      {key, value}                -> {key , encode_value(value, params)}
    end)
  end

  def encode_value(list, params) when is_list(list),
    do: Enum.map(list, &encode_value(&1, params))
  def encode_value({:^, _, [idx]}, params), do: elem(params, idx) |> encode_value(params)
  def encode_value(value, _), do: encode_value(value)

  def encode_value(int) when is_integer(int), do: int
  def encode_value(float) when is_float(float), do: float
  def encode_value(string) when is_binary(string), do: string
  def encode_value(atom) when is_atom(atom), do: atom
  def encode_value(list) when is_list(list), do: Enum.map(list, &encode_value/1)
  def encode_value(%BSON.ObjectId{} = objectid), do: objectid
  def encode_value(%Tagged{value: value, type: type}), do: typed_value(value, type)
  def encode_value({{_, _, _} = date, {hour, min, sec, usec}}) do
    seconds = :calendar.datetime_to_gregorian_seconds({date, {hour, min, sec}})
    %BSON.DateTime{utc: seconds * 1000 + div(usec, 1000)}
  end
  def encode_value({_, _, _} = ast) do
    raise ArgumentError, "Mongodb adapter does not support `#{Macro.to_string(ast)}` " <>
      "in the place used, a value was expected."
  end
  def encode_value(%Decimal{}) do
    raise ArgumentError, "Mongodb adapter does not support decimal values."
  end
  def encode_value(value) do
    raise ArgumentError, "Mongodb adapter does not support `#{inspect value}` " <>
      "in the place used, a value was expected."
  end

  defp typed_value(nil, _), do: nil
  defp typed_value(value, {:array, type}), do: Enum.map(value, &typed_value(&1, type))
  defp typed_value(value, :binary), do: %BSON.Binary{binary: value}
  defp typed_value(value, :uuid), do: %BSON.Binary{binary: value, subtype: :uuid}
  defp typed_value(value, :object_id), do: %BSON.ObjectId{value: value}
end
