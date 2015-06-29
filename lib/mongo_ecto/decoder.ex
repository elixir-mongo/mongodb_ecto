defmodule Mongo.Ecto.Decoder do
  @moduledoc false

  import Mongo.Ecto.Utils

  def decode_document(document, pk) do
    Enum.into(document, %{}, fn
      {"_id", value} -> {Atom.to_string(pk), decode_value(value, pk)}
      {key, value}   -> {key, decode_value(value, pk)}
    end)
  end

  def decode_value(int, _pk) when is_integer(int),
    do: int
  def decode_value(atom, _pk) when is_atom(atom),
    do: atom
  def decode_value(float, _pk) when is_float(float),
    do: float
  def decode_value(string, _pk) when is_binary(string),
    do: string
  def decode_value(keyword, pk) when is_keyword(keyword),
    do: decode_document(keyword, pk)
  def decode_value(list, pk) when is_list(list),
    do: Enum.map(list, &decode_value(&1, pk))
  def decode_value(%BSON.Binary{binary: value}, _pk),
    do: value
  def decode_value(%BSON.ObjectId{value: value}, _pk),
    do: value
  def decode_value(%BSON.DateTime{} = datetime, _pk),
    do: BSON.DateTime.to_datetime(datetime)
  def decode_value(map, pk) when is_map(map),
    do: decode_document(map, pk)
end
