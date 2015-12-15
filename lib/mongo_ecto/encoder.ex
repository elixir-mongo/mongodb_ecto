defmodule Mongo.Ecto.Encoder do
  @moduledoc false

  import Mongo.Ecto.Utils
  alias Ecto.Query.Tagged

  def encode(doc, params, pk) when is_keyword(doc),
    do: document(doc, params, pk)
  def encode(list, params, pk) when is_list(list),
    do: map(list, &encode(&1, params, pk))
  def encode({:^, _, [idx]}, params, pk),
    do: elem(params, idx) |> encode(params, pk)
  def encode(%Tagged{value: value, type: type}, params, _pk),
    do: {:ok, typed_value(value, type, params)}
  def encode(%{__struct__: _} = struct, _params, pk),
    do: encode(struct, pk) # Pass down other structs
  def encode(map, params, pk) when is_map(map),
    do: document(map, params, pk)
  def encode(value, _params, pk),
    do: encode(value, pk)

  @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  def encode(doc, pk) when is_keyword(doc),
    do: document(doc, pk)
  def encode(int, _pk) when is_integer(int),
    do: {:ok, int}
  def encode(float, _pk) when is_float(float),
    do: {:ok, float}
  def encode(string, _pk) when is_binary(string),
    do: {:ok, string}
  def encode(atom, _pk) when is_atom(atom),
    do: {:ok, atom}
  def encode(list, pk) when is_list(list),
    do: map(list, &encode(&1, pk))
  def encode(%BSON.ObjectId{} = objectid, _pk),
    do: {:ok, objectid}
  def encode(%BSON.JavaScript{} = js, _pk),
    do: {:ok, js}
  def encode(%BSON.Regex{} = regex, _pk),
    do: {:ok, regex}
  def encode(%BSON.DateTime{} = datetime, _pk),
    do: {:ok, datetime}
  def encode(%BSON.Binary{} = binary, _pk),
    do: {:ok, binary}
  def encode(%Tagged{value: value, type: type}, _pk),
    do: {:ok, typed_value(value, type)}
  def encode(%{__struct__: change, field: field, value: value}, pk)
      when change in [Mongo.Ecto.ChangeMap, Mongo.Ecto.ChangeArray] do
    case encode(value, pk) do
      {:ok, value} -> {:ok, {field, value}}
      :error       -> :error
    end
  end
  def encode(%{__struct__: _}, _pk),
    do: :error # Other structs are not supported
  def encode(map, pk) when is_map(map),
    do: document(map, pk)
  def encode({{_, _, _} = date, {hour, min, sec, usec}}, _pk) do
    seconds =
      :calendar.datetime_to_gregorian_seconds({date, {hour, min, sec}}) - @epoch
    {:ok, %BSON.DateTime{utc: seconds * 1000 + div(usec, 1000)}}
  end
  def encode(_value, _pk) do
    :error
  end

  defp document(doc, pk) do
    map(doc, fn {key, value} ->
      pair(key, value, pk, &encode(&1, pk))
    end)
  end

  defp document(doc, params, pk) do
    map(doc, fn {key, value} ->
      pair(key, value, pk, &encode(&1, params, pk))
    end)
  end

  defp pair(key, value, pk, fun) do
    case fun.(value) do
      {:ok, {subkey, encoded}} -> {:ok, {"#{key}.#{subkey}", encoded}}
      {:ok, encoded} -> {:ok, {key(key, pk), encoded}}
      :error         -> :error
    end
  end

  defp key(pk, pk), do: :_id
  defp key(key, _), do: key

  defp typed_value({:^, _, [idx]}, type, params),
    do: typed_value(elem(params, idx), type)
  defp typed_value(value, type, _params),
    do: typed_value(value, type)

  defp typed_value(nil, _),
    do: nil
  defp typed_value(value, :any),
    do: value
  defp typed_value(value, {:array, type}),
    do: Enum.map(value, &typed_value(&1, type))
  defp typed_value(value, :binary),
    do: %BSON.Binary{binary: value}
  defp typed_value(value, :uuid),
    do: %BSON.Binary{binary: value, subtype: :uuid}
  defp typed_value(value, :binary_id),
    do: %BSON.ObjectId{value: value}

  defp map(list, fun) do
    return =
      Enum.flat_map_reduce(list, :ok, fn elem, :ok ->
        case fun.(elem) do
          {:ok, value} -> {[value], :ok}
          :error       -> {:halt, :error}
        end
      end)

    case return do
      {values,  :ok}    -> {:ok, values}
      {_values, :error} -> :error
    end
  end
end
