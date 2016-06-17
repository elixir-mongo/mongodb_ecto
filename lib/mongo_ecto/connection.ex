defmodule Mongo.Ecto.Connection do
  @moduledoc false

  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery
  alias Mongo.Ecto.NormalizedQuery.CommandQuery
  alias Mongo.Ecto.NormalizedQuery.CountQuery
  alias Mongo.Ecto.NormalizedQuery.AggregateQuery

  ## Worker

  def storage_down(opts) do
    opts = Keyword.put(opts, :size, 1)

    {:ok, _} = Mongo.Ecto.AdminPool.start_link(opts)

    try do
      Mongo.run_command(Mongo.Ecto.AdminPool, dropDatabase: 1)
      :ok
    after
      true = Mongo.Ecto.AdminPool.stop
    end
  end

  ## Callbacks for adapter

  def read(conn, query, opts \\ [])

  def read(conn, %ReadQuery{} = query, opts) do
    opts  = [projection: query.projection, sort: query.order] ++ query.opts ++ opts
    coll  = query.coll
    query = query.query

    Mongo.find(conn, coll, query, opts)
  end

  def read(conn, %CountQuery{} = query, opts) do
    coll  = query.coll
    opts  = query.opts ++ opts
    query = query.query

    [%{"value" => Mongo.count!(conn, coll, query, opts)}]
  end

  def read(conn, %AggregateQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    pipeline = query.pipeline

    Mongo.aggregate(conn, coll, pipeline, opts)
  end

  def delete_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    query    = query.query

    %{deleted_count: n} = Mongo.delete_many!(conn, coll, query, opts)
    n
  end

  def delete(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    query    = query.query

    case Mongo.delete_one(conn, coll, query, opts) do
      {:ok, %{deleted_count: 1}} ->
        {:ok, []}
      {:ok, _} ->
        {:error, :stale}
      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def update_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    query    = query.query

    %{modified_count: n} = Mongo.update_many!(conn, coll, query, command, opts)
    n
  end

  def update(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    query    = query.query

    case Mongo.update_one(conn, coll, query, command, opts) do
      {:ok, %{modified_count: 1}} ->
        {:ok, []}
      {:ok, _} ->
        {:error, :stale}
      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def insert(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts

    case Mongo.insert_one(conn, coll, command, opts) do
      {:ok, result}   -> {:ok, result}
      {:error, error} -> check_constraint_errors(error)
    end
  end

  def command(conn, %CommandQuery{} = query, opts) do
    command  = query.command
    opts     = query.opts ++ opts

    Mongo.run_command!(conn, command, opts)
  end

  defp check_constraint_errors(%Mongo.Error{code: 11000, message: msg}) do
    {:invalid, [unique: extract_index(msg)]}
  end
  defp check_constraint_errors(other) do
    raise other
  end

  defp extract_index(msg) do
    parts = String.split(msg, [".$", "index: ", " dup "])

    case Enum.reverse(parts) do
      [_, index | _] ->
        String.strip(index)
      _  ->
        raise "failed to extract index from error message: #{inspect msg}"
    end
  end
end
