defmodule Mongo.Ecto.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.Worker

  alias Mongo.ReadResult
  alias Mongo.WriteResult

  ## Worker

  def connect(opts) do
    Mongo.Connection.start_link(opts)
  end

  def disconnect(conn) do
    Mongo.Connection.stop(conn)
  end

  ## Callbacks for adapter

  def all(conn, query, opts) do
    {coll, query, projector, opts} = extract(:all, query, opts)
    Mongo.find(conn, coll, query, projector, opts)
    |> read_result
  end

  def delete_all(conn, query, opts) do
    {coll, query, opts} = extract(:delete_all, query, opts)
    Mongo.remove(conn, coll, query, opts)
    |> multiple_write_result
  end

  def delete(conn, coll, query, opts) do
    Mongo.remove(conn, coll, query, opts)
    |> single_write_result
  end

  def update_all(conn, query, opts) do
    {coll, query, command, opts} = extract(:update_all, query, opts)
    Mongo.update(conn, coll, query, command, opts)
    |> multiple_write_result
  end

  def update(conn, coll, query, command, opts) do
    Mongo.update(conn, coll, query, command, opts)
    |> single_write_result
  end

  def insert(conn, coll, doc, opts) do
    Mongo.insert(conn, coll, doc, opts)
    |> single_write_result
  end

  def command(conn, command, opts) do
    opts = [num_return: -1] ++ opts
    Mongo.find(conn, "$cmd", command, %{}, opts)
    |> read_result
  end

  defp extract(:all, query, opts) do
    {coll, _, _} = query.from
    opts = [num_return: query.num_return, num_skip: query.num_skip] ++ opts

    {coll, query.query_order, query.projection, opts}
  end
  defp extract(:delete_all, query, opts) do
    {coll, _, _} = query.from
    opts = [multi: true] ++ opts

    {coll, query.query_order, opts}
  end
  defp extract(:update_all, query, opts) do
    {coll, _, _} = query.from
    opts = [multi: true] ++ opts
    command = %{"$set": query.command}

    {coll, query.query_order, command, opts}
  end

  defp read_result({:ok, %ReadResult{docs: docs}}) do
    {:ok, docs}
  end
  defp read_result({:error, _} = error) do
    error
  end

  defp single_write_result({:ok, %WriteResult{num_inserted: 1}}) do
    {:ok, []}
  end
  defp single_write_result({:ok, %WriteResult{num_matched: 1}}) do
    {:ok, []}
  end
  defp single_write_result({:ok, %WriteResult{num_matched: 0}}) do
    {:error, :stale}
  end
  defp single_write_result({:error, _} = error) do
    error
  end

  defp multiple_write_result({:ok, %WriteResult{num_removed: n}}) when is_integer(n) do
    {:ok, n}
  end
  defp multiple_write_result({:ok, %WriteResult{num_matched: n}}) when is_integer(n) do
    {:ok, n}
  end
  defp multiple_write_result({:error, _} = error) do
    error
  end
end
