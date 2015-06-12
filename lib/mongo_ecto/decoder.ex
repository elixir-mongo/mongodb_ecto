defmodule Mongo.Ecto.Decoder do
  @moduledoc false

  def decode_document(document, pk) do
    Enum.into(document, %{}, fn
      {"_id", value} -> {Atom.to_string(pk), decode_value(value)}
      {key, value}   -> {key, decode_value(value)}
    end)
  end

  def decode_value(int) when is_integer(int), do: int
  def decode_value(atom) when is_atom(atom), do: atom
  def decode_value(float) when is_float(float), do: float
  def decode_value(string) when is_binary(string), do: string
  def decode_value(list) when is_list(list), do: Enum.map(list, &decode_value/1)
  def decode_value(%BSON.Binary{binary: value}), do: value
  def decode_value(%BSON.ObjectId{value: value}), do: value
  def decode_value(%BSON.DateTime{utc: utc}) do
    seconds = div(utc, 1000)
    usec = rem(utc, 1000) * 1000
    {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(seconds)
    {date, {hour, min, sec, usec}}
  end
end
