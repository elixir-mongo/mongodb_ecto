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
    {:ok, result} = Mongo.find(conn, coll, query, projector, opts)
    result.docs
  end

  def delete_all(conn, query, opts) do
    {coll, query, opts} = extract(:delete_all, query, opts)
    Mongo.remove(conn, coll, query, opts)
    |> multiple_result
  end

  def delete(conn, coll, query, opts) do
    Mongo.remove(conn, coll, query, opts)
    |> single_result
  end

  def update_all(conn, query, opts) do
    {coll, query, command, opts} = extract(:update_all, query, opts)
    Mongo.update(conn, coll, query, command, opts)
    |> multiple_result
  end

  def update(conn, coll, query, command, opts) do
    Mongo.update(conn, coll, query, command, opts)
    |> single_result
  end

  def insert(conn, coll, doc, opts) do
    Mongo.insert(conn, coll, doc, opts)
  end

  def command(conn, command) do
    case Mongo.find(conn, "$cmd", command, %{}, num_return: -1) do
      {:ok, %ReadResult{docs: docs}} ->
        {:ok, docs}
      {:error, _} = error ->
        error
    end
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

  defp single_result({:ok, %WriteResult{num_matched: 1}}) do
    {:ok, []}
  end
  defp single_result({:ok, %WriteResult{num_matched: 0}}) do
    {:error, :stale}
  end
  defp single_result({:error, _} = error) do
    error
  end

  defp multiple_result({:ok, %WriteResult{num_removed: n}}) when is_integer(n) do
    n
  end
  defp multiple_result({:ok, %WriteResult{num_matched: n}}) when is_integer(n) do
    n
  end
  defp multiple_result({:error, _} = error) do
    error
  end
end
