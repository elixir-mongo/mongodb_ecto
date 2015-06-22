defmodule Mongo.Ecto.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.Worker

  alias Mongo.ReadResult
  alias Mongo.WriteResult

  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery

  ## Worker

  def connect(opts) do
    Mongo.Connection.start_link(opts)
  end

  def disconnect(conn) do
    Mongo.Connection.stop(conn)
  end

  ## Callbacks for adapter

  def all(conn, query, opts \\ []) do
    %ReadQuery{from: {coll, _, _}, query: query, projection: projection,
              opts: query_opts} = query

    Mongo.find(conn, coll, query, projection, query_opts ++ opts)
    |> read_result
  end

  def delete_all(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, opts: query_opts} = query

    Mongo.remove(conn, coll, query, query_opts ++ opts)
    |> multiple_write_result
  end

  def delete(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, opts: query_opts} = query

    Mongo.remove(conn, coll, query, query_opts ++ opts)
    |> single_write_result
  end

  def update_all(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, command: command, opts: query_opts} = query

    Mongo.update(conn, coll, query, command, query_opts ++ opts)
    |> multiple_write_result
  end

  def update(conn, query, opts) do
    %WriteQuery{coll: coll, query: query, command: command, opts: query_opts} = query

    Mongo.update(conn, coll, query, command, query_opts ++ opts)
    |> single_write_result
  end

  def insert(conn, query, opts) do
    %WriteQuery{coll: coll, command: command, opts: query_opts} = query

    Mongo.insert(conn, coll, command, query_opts ++ opts)
    |> single_write_result
  end

  def command(conn, command, opts) do
    opts = [num_return: -1] ++ opts
    Mongo.find(conn, "$cmd", command, %{}, opts)
    |> read_result
  end

  defp read_result({:ok, %ReadResult{docs: docs}}),
    do: {:ok, docs}
  defp read_result({:error, error}),
    do: error(error)

  defp single_write_result({:ok, %WriteResult{num_inserted: 1}}),
    do: {:ok, []}
  defp single_write_result({:ok, %WriteResult{num_matched: 1}}),
    do: {:ok, []}
  defp single_write_result({:ok, %WriteResult{num_matched: 0}}),
    do: {:error, :stale}
  defp single_write_result({:error, error}),
    do: error(error)

  defp multiple_write_result({:ok, %WriteResult{num_removed: n}}) when is_integer(n),
    do: n
  defp multiple_write_result({:ok, %WriteResult{num_matched: n}}) when is_integer(n),
    do: n
  defp multiple_write_result({:error, error}),
    do: error(error)

  defp error(error) do
    if Exception.exception?(error) do
      raise error
    else
      {:error, error}
    end
  end
end
