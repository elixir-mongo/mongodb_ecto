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

  def all(conn, query, opts \\ []) do
    %ReadQuery{coll: coll, query: query, projection: projection, opts: query_opts} = query

    Mongo.find(conn, coll, query, projection, query_opts ++ opts)
    |> read_result
  end

  def delete_all(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, opts: query_opts} = query

    Mongo.remove(conn, coll, query, query_opts ++ opts)
    |> write_result
  end

  def delete(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, opts: query_opts} = query

    Mongo.remove(conn, coll, query, query_opts ++ opts)
    |> write_result
  end

  def update_all(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, command: command, opts: query_opts} = query

    Mongo.update(conn, coll, query, command, query_opts ++ opts)
    |> write_result
  end

  def update(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, command: command, opts: query_opts} = query

    Mongo.update(conn, coll, query, command, query_opts ++ opts)
    |> write_result
  end

  def insert(conn, query, opts) do
    %WriteQuery{coll: coll, command: command, opts: query_opts} = query

    Mongo.insert(conn, coll, command, query_opts ++ opts)
    |> write_result
  end

  def command(conn, query, opts) do
    %CommandQuery{command: command, database: db, opts: query_opts} = query

    with_database(conn, db, fn ->
      Mongo.find(conn, "$cmd", command, %{}, query_opts ++ opts)
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
  defp read_result({:error, _} = error),
    do: error

  defp write_result({:ok, %WriteResult{num_inserted: n}}) when is_integer(n),
    do: {:ok, n}
  defp write_result({:ok, %WriteResult{num_removed: n}}) when is_integer(n),
    do: {:ok, n}
  defp write_result({:ok, %WriteResult{num_matched: n}}) when is_integer(n),
    do: {:ok, n}
  defp write_result({:error, _} = error),
    do: error
end
