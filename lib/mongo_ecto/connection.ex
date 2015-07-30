defmodule Mongo.Ecto.Connection do
  @moduledoc false

  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery
  alias Mongo.Ecto.NormalizedQuery.CommandQuery
  alias Mongo.Ecto.NormalizedQuery.CountQuery

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

  def all(conn, %ReadQuery{} = query, opts \\ []) do
    opts  = [projection: query.projection, sort: query.order] ++ query.opts ++ opts
    coll  = query.coll
    query = query.query

    Mongo.find(conn, coll, query, opts)
  end

  def delete_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    query    = query.query

    case Mongo.delete_many(conn, coll, query, opts) do
      {:ok, %{deleted_count: n}} -> n
    end
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
    end
  end

  def update_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    query    = query.query

    case Mongo.update_many(conn, coll, query, command, opts) do
      {:ok, %{modified_count: n}} -> n
    end
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
    end
  end

  def insert(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts

    Mongo.insert_one(conn, coll, command, opts)
  end

  def command(conn, %CommandQuery{} = query, opts) do
    command  = query.command
    opts     = query.opts ++ opts

    Mongo.run_command(conn, command, opts)
  end

  def count(conn, %CountQuery{} = query, opts) do
    coll  = query.coll
    opts  = query.opts ++ opts
    query = query.query

    Mongo.count(conn, coll, query, opts)
  end
end
