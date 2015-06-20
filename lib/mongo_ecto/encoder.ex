defmodule Mongo.Ecto.Encoder do
  @moduledoc false

  alias Ecto.Query.Tagged

  def encode_document(doc, pk) do
    map(doc, fn {key, value} ->
      encode_pair(key, value, pk, &encode_value/1)
    end)
  end
  def encode_document(doc, params, pk) do
    map(doc, fn {key, value} ->
      encode_pair(key, value, pk, &encode_value(&1, params))
    end)
  end

  defp key(pk, pk), do: :_id
  defp key(key, _), do: key

  defp encode_pair(key, value, pk, fun) do
    case fun.(value) do
      {:ok, encoded}    -> {:ok, {key(key, pk), encoded}}
      {:error, _} = err -> err
    end
  end

  def encode_value(list, params) when is_list(list),
    do: map(list, &encode_value(&1, params))
  def encode_value({:^, _, [idx]}, params),
    do: elem(params, idx) |> encode_value(params)
  def encode_value(%Tagged{value: value, type: type}, params),
    do: {:ok, typed_value(value, type, params)}
  def encode_value(value, _),
    do: encode_value(value)

  def encode_value(int) when is_integer(int),
    do: {:ok, int}
  def encode_value(float) when is_float(float),
    do: {:ok, float}
  def encode_value(string) when is_binary(string),
    do: {:ok, string}
  def encode_value(atom) when is_atom(atom),
    do: {:ok, atom}
  def encode_value(list) when is_list(list),
    do: map(list, &encode_value/1)
  def encode_value(%BSON.ObjectId{} = objectid),
    do: {:ok, objectid}
  def encode_value(%Tagged{value: value, type: type}),
    do: {:ok, typed_value(value, type)}
  def encode_value({{_, _, _} = date, {hour, min, sec, usec}}) do
    seconds = :calendar.datetime_to_gregorian_seconds({date, {hour, min, sec}})
    {:ok, %BSON.DateTime{utc: seconds * 1000 + div(usec, 1000)}}
  end
  def encode_value(value) do
    {:error, value}
  end

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
  defp typed_value(value, :object_id),
    do: %BSON.ObjectId{value: value}

  defp map(list, fun) do
    return =
      Enum.flat_map_reduce(list, :ok, fn elem, :ok ->
        case fun.(elem) do
          {:ok, value}      -> {[value], :ok}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case return do
      {values, :ok} ->
        {:ok, values}
      {_values, {:error, _} = err} ->
        err
    end
  end
end
