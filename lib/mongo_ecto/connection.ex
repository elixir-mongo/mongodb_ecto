defmodule Mongo.Ecto.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.Connection

  alias Mongo.ReadResult
  alias Mongo.WriteResult

  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery
  alias Mongo.Ecto.NormalizedQuery.CommandQuery

  ## Worker

  def connect(opts) do
    Mongo.Connection.start_link(opts)
  end

  def disconnect(conn) do
    Mongo.Connection.stop(conn)
  end

  ## Callbacks for adapter

  def all(conn, %ReadQuery{} = query, opts \\ []) do
    coll       = query.coll
    projection = query.projection
    opts       = query.opts ++ opts
    database   = query.database
    query      = query.query

    with_database(conn, database, fn ->
      Mongo.find(conn, coll, query, projection, opts)
    end)
    |> read_result
  end

  def delete_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    database = query.database
    query    = query.query

    with_database(conn, database, fn ->
      Mongo.remove(conn, coll, query, opts)
    end)
    |> write_result
  end

  def delete(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    database = query.database
    query    = query.query

    with_database(conn, database, fn ->
      Mongo.remove(conn, coll, query, opts)
    end)
    |> write_result
  end

  def update_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    database = query.database
    query    = query.query

    with_database(conn, database, fn ->
      Mongo.update(conn, coll, query, command, opts)
    end)
    |> write_result
  end

  def update(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    database = query.database
    query    = query.query

    with_database(conn, database, fn ->
      Mongo.update(conn, coll, query, command, opts)
    end)
    |> write_result
  end

  def insert(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    database = query.database

    with_database(conn, database, fn ->
      Mongo.insert(conn, coll, command, opts)
    end)
    |> write_result
  end

  def command(conn, %CommandQuery{} = query, opts) do
    command  = query.command
    database = query.database
    opts     = query.opts ++ opts

    with_database(conn, database, fn ->
      Mongo.find(conn, "$cmd", command, %{}, opts)
    end)
    |> read_result
  end

  defp with_database(_conn, nil, fun), do: fun.()
  defp with_database(conn, db, fun) do
    olddb = Mongo.database(conn)
    Mongo.database(conn, db)
    try do
      fun.()
    after
      Mongo.database(conn, olddb)
    end
  end

  defp read_result({:ok, %ReadResult{docs: docs}}),
    do: {:ok, docs}
  defp read_result(%Mongo.Error{} = error),
    do: {:error, error}

  defp write_result({:ok, %WriteResult{num_inserted: n}}) when is_integer(n),
    do: {:ok, n}
  defp write_result({:ok, %WriteResult{num_removed: n}}) when is_integer(n),
    do: {:ok, n}
  defp write_result({:ok, %WriteResult{num_matched: n}}) when is_integer(n),
    do: {:ok, n}
  defp write_result(%Mongo.Error{} = error),
    do: {:error, error}
end
