defmodule Mongo.Ecto.Conversions do
  @moduledoc false

  import Mongo.Ecto.Utils

  def to_ecto_pk(%Ecto.Query.Tagged{type: type, value: value}) do
    {:ok, dumped} = Ecto.Type.adapter_dump(Mongo.Ecto, type, value)
    dumped
  end
  def to_ecto_pk(%{__struct__: _} = value, _pk),
    do: value
  def to_ecto_pk(map, pk) when is_map(map) do
    Enum.into(map, %{}, fn
      {"_id", value} -> {Atom.to_string(pk), to_ecto_pk(value, pk)}
      {key, value}   -> {key, to_ecto_pk(value, pk)}
    end)
  end
  def to_ecto_pk(list, pk) when is_list(list),
    do: Enum.map(list, &to_ecto_pk(&1, pk))
  def to_ecto_pk(value, _pk),
    do: value

  def inject_params(doc, params, pk) when is_keyword(doc),
    do: document(doc, params, pk)
  def inject_params(list, params, pk) when is_list(list),
    do: map(list, &inject_params(&1, params, pk))
  def inject_params(%Ecto.Query.Tagged{tag: tag, type: type, value: {:^, _, [idx]} = value}, params, pk) do
    ## If we need to cast the values of the return, they should go here
    elem(params, idx) |> inject_params(params, pk)
  end
  def inject_params({:^, _, [idx]}, params, pk),
    do: elem(params, idx) |> inject_params(params, pk)
  def inject_params(%{__struct__: _} = struct, _params, pk),
    do: from_ecto_pk(struct, pk)
  def inject_params(map, params, pk) when is_map(map),
    do: document(map, params, pk)
  def inject_params(value, _params, pk),
    do: from_ecto_pk(value, pk)

  def from_ecto_pk(%{__struct__: change, field: field, value: value}, pk)
      when change in [Mongo.Ecto.ChangeMap, Mongo.Ecto.ChangeArray] do
    case from_ecto_pk(value, pk) do
      {:ok, value} -> {:ok, {field, value}}
      :error       -> :error
    end
  end

  def from_ecto_pk(%Ecto.Query.Tagged{tag: :binary_id, value: value}, _pk),
    do: {:ok, BSON.Decoder.decode(value)}
  def from_ecto_pk(%Ecto.Query.Tagged{type: type, value: value}, _pk),
    do: Ecto.Type.adapter_dump(Mongo.Ecto, type, value)
  def from_ecto_pk(%Mongo.Ecto.Regex{} = regex, _pk),
    do: Mongo.Ecto.Regex.dump(regex)
  def from_ecto_pk(%{__struct__: _} = value, _pk),
    do: {:ok, value}
  def from_ecto_pk(map, pk) when is_map(map),
    do: document(map, pk)
  def from_ecto_pk(keyword, pk) when is_keyword(keyword),
    do: document(keyword, pk)
  def from_ecto_pk(list, pk) when is_list(list),
    do: map(list, &from_ecto_pk(&1, pk))
  def from_ecto_pk(value, _pk) when is_literal(value),
    do: {:ok, value}
  def from_ecto_pk({{_,_,_},{_,_,_,_}} = value, _pk),
    do: Ecto.Type.adapter_dump(Mongo.Ecto, :datetime, value)
  def from_ecto_pk({_,_,_} = value, _pk),
    do: Ecto.Type.adapter_dump(Mongo.Ecto, :date, value)
  def from_ecto_pk({_,_,_,_} = value, _pk),
    do: Ecto.Type.adapter_dump(Mongo.Ecto, :time, value)
  def from_ecto_pk(_value, _pk),
    do: :error

  defp document(doc, pk) do
    map(doc, fn {key, value} ->
      pair(key, value, pk, &from_ecto_pk(&1, pk))
    end)
  end

  defp document(doc, params, pk) do
    map(doc, fn {key, value} ->
      pair(key, value, pk, &inject_params(&1, params, pk))
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

  defp map(map, _fun) when is_map(map) and map_size(map) == 0 do
    {:ok, %{}}
  end
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
