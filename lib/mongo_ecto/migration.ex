defmodule Mongo.Ecto.Migration do
  @moduledoc false

  @behaviour Ecto.Adapter.Migration

  @impl true
  def supports_ddl_transaction?, do: false

  # No-ops: MongoDB is schema-less; column-level DDL has no equivalent.
  @impl true
  def lock_for_migrations(_meta, _opts, fun), do: fun.()

  @impl true
  def execute_ddl(_meta, {:alter, %Ecto.Migration.Table{}, _changes}, _opts), do: {:ok, []}
  def execute_ddl(_meta, {:rename, %Ecto.Migration.Table{}, %Ecto.Migration.Table{}}, _opts), do: {:ok, []}
  def execute_ddl(_meta, {:rename, %Ecto.Migration.Table{}, _old_col, _new_col}, _opts), do: {:ok, []}
  def execute_ddl(_meta, {:rename, %Ecto.Migration.Index{}, _new_name}, _opts), do: {:ok, []}
  def execute_ddl(%{pid: pool}, {:create_if_not_exists, %Ecto.Migration.Table{name: name}, commands}, _opts) do
    collection = to_string(name)

    case Mongo.create(pool, collection) do
      :ok -> :ok
      {:error, %Mongo.Error{code: 48}} -> :ok
      {:error, reason} -> raise reason
    end

    # Skip :binary_id and :uuid pks — they map to MongoDB's _id which is auto-indexed.
    # Only create a unique index for non-_id primary keys (e.g. :integer version field).
    pk_fields = for {:add, field, type, opts} <- commands,
                    is_list(opts) and Keyword.get(opts, :primary_key, false),
                    type not in [:binary_id, :uuid],
                    do: to_string(field)

    if pk_fields != [] do
      key = pk_fields |> Enum.map(fn f -> {f, 1} end) |> Map.new()
      index_def = %{key: key, unique: true, name: "#{collection}_pk"}
      case Mongo.create_indexes(pool, collection, [index_def]) do
        :ok -> :ok
        {:error, reason} -> raise reason
      end
    end

    {:ok, []}
  end
  def execute_ddl(_meta, {:drop_if_exists, %Ecto.Migration.Table{}, _mode}, _opts), do: {:ok, []}
  def execute_ddl(_meta, {:create_if_not_exists, %Ecto.Migration.Index{}}, _opts), do: {:ok, []}
  def execute_ddl(_meta, {:drop_if_exists, %Ecto.Migration.Index{}, _mode}, _opts), do: {:ok, []}
  def execute_ddl(%{pid: pool}, {:create, %Ecto.Migration.Table{name: name}, _columns}, _opts) do
    case Mongo.create(pool, to_string(name)) do
      :ok -> {:ok, []}
      {:error, %Mongo.Error{code: 48}} -> {:ok, []}
      {:error, reason} -> raise reason
    end
  end

  def execute_ddl(%{pid: pool}, {:drop, %Ecto.Migration.Table{name: name}, _mode}, _opts) do
    Mongo.drop_collection(pool, to_string(name))
    {:ok, []}
  end
  def execute_ddl(%{pid: pool}, {:create, %Ecto.Migration.Index{} = index}, _opts) do
    collection = to_string(index.table)
    key = index.columns |> Enum.map(fn col -> {to_string(col), 1} end) |> Map.new()
    index_def = %{key: key, name: index_name(index), unique: index.unique || false, sparse: false}

    case Mongo.create_indexes(pool, collection, [index_def]) do
      :ok -> {:ok, []}
      {:error, reason} -> raise reason
    end
  end

  def execute_ddl(%{pid: pool}, {:drop, %Ecto.Migration.Index{} = index, _mode}, _opts) do
    case Mongo.drop_index(pool, to_string(index.table), index_name(index)) do
      :ok -> {:ok, []}
      {:error, reason} -> raise reason
    end
  end

  def execute_ddl(_meta, {:create, %Ecto.Migration.Constraint{}}, _opts), do: {:ok, []}
  def execute_ddl(_meta, {:drop, %Ecto.Migration.Constraint{}, _mode}, _opts), do: {:ok, []}
  def execute_ddl(_meta, string, _opts) when is_binary(string), do: {:ok, []}
  def execute_ddl(_meta, keyword, _opts) when is_list(keyword), do: {:ok, []}

  defp index_name(%Ecto.Migration.Index{name: name}) when not is_nil(name),
    do: to_string(name)

  defp index_name(%Ecto.Migration.Index{table: table, columns: columns}),
    do: "#{to_string(table)}_#{columns |> Enum.map(&to_string/1) |> Enum.join("_")}_index"
end
