defmodule Mongo.Ecto.DiffEmbed do

  defstruct changes: []

  alias __MODULE__
  alias Ecto.Embedded
  alias Ecto.Changeset

  def load(%Embedded{cardinality: :one, embed: embed, field: field}, data, loader) do
    {:ok, load(field, embed, data, loader)}
  end

  def load(%Embedded{cardinality: :many, embed: embed, field: field}, data, loader) do
    data
    |> Enum.map(&elem(&1, 1))
    |> array(&load(field, embed, &1, loader), [])
  end

  def load(_embed, _data, _loader), do: :error

  defp load(_field, model, value, loader) when is_map(value) do
    Ecto.Schema.__load__(model, nil, nil, value, loader)
  end

  defp load(field, _model, value, _loader) do
    raise ArgumentError, "cannot load embed `#{field}`, invalid value: #{inspect value}"
  end

  def dump(%Embedded{cardinality: :one} = embed, nil, dumper) do
    Ecto.Type.dump({:embed, embed}, nil, dumper)
  end

  def dump(%Embedded{cardinality: :one, embed: embed, field: _field}, data, dumper)
      when is_map(data) do
    {:ok, dump(embed, primary_key(embed), data, dumper)}
  end

  defp dump(_model, _pk, %Changeset{action: :insert} = changeset, dumper) do
    %{model: struct, changes: changes, types: types} = changeset

    Enum.map(types, fn {field, type} ->
      value =
        case Map.fetch(changes, field) do
          {:ok, change} -> change
          :error        -> Map.get(struct, field)
        end

      case dumper.(type, value) do
        {:ok, value} ->
          {field, value}
        :error ->
          raise ArgumentError, "cannot dump `#{inspect value}` as type #{inspect type}"
      end
    end)
  end

  defp dump(_model, _pk, %Changeset{action: :update} = changeset, dumper) do
    %{changes: changes, types: types} = changeset

    changes =
      Enum.flat_map(changes, fn {field, value} ->
        type = Map.get(types, field)

        case dumper.(type, value) do
          {:ok, %DiffEmbed{changes: changes}} ->
            Enum.map(changes, fn {key, value} ->
              {"#{field}.#{key}", value}
            end)
          {:ok, value} ->
            [{field, value}]
          :error ->
            raise ArgumentError, "cannot dump `#{inspect value}` as type #{inspect type}"
        end
      end)

    %DiffEmbed{changes: changes}
  end

  defp dump(_model, _pk, %Changeset{action: :delete}, _dumper), do: nil

  defp primary_key(module) do
    case module.__schema__(:primary_key) do
      [pk] -> pk
      _    -> raise ArgumentError,
                "embeded models must have exactly one primary key field"
    end
  end

  defp array([h|t], fun, acc) do
    case fun.(h) do
      {:ok, h} -> array(t, fun, [h|acc])
      :error   -> :error
    end
  end

  defp array([], _fun, acc) do
    {:ok, Enum.reverse(acc)}
  end
end
