defmodule Mongo.Ecto.Connection do
  @moduledoc false

  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery
  alias Mongo.Ecto.NormalizedQuery.CommandQuery
  alias Mongo.Ecto.NormalizedQuery.CountQuery
  alias Mongo.Ecto.NormalizedQuery.AggregateQuery

  ## Worker

  def storage_down(opts) do
    opts = Keyword.put(opts, :pool, DBConnection.Connection)

    {:ok, conn} = Mongo.start_link(opts)

    try do
      Mongo.command!(conn, dropDatabase: 1)
      :ok
    after
      GenServer.stop(conn)
    end
  end

  ## Callbacks for adapter

  def read(repo, query, opts \\ [])

  def read(repo, %ReadQuery{} = query, opts) do
    projection = Map.put_new(query.projection, :_id, false)
    opts  = [projection: projection, sort: query.order] ++ query.opts ++ opts
    coll  = query.coll
    query = query.query

    query(repo, :find, [coll, query], opts)
  end

  def read(repo, %CountQuery{} = query, opts) do
    coll  = query.coll
    opts  = query.opts ++ opts
    query = query.query

    [%{"value" => query(repo, :count!, [coll, query], opts)}]
  end

  def read(repo, %AggregateQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    pipeline = query.pipeline

    query(repo, :aggregate, [coll, pipeline], opts)
  end

  def delete_all(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    query    = query.query

    %{deleted_count: n} = query(repo, :delete_many!, [coll, query], opts)
    n
  end

  def delete(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = query.opts ++ opts
    query    = query.query

    case query(repo, :delete_one, [coll, query], opts) do
      {:ok, %{deleted_count: 1}} ->
        {:ok, []}
      {:ok, _} ->
        {:error, :stale}
      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def update_all(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    query    = query.query

    case query(repo, :update_many, [coll, query, command], opts) do
      {:ok, %Mongo.UpdateResult{modified_count: m}} ->
        m
      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def update(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts
    query    = query.query

    case query(repo, :update_one, [coll, query, command], opts) do
      {:ok, %{modified_count: 1}} ->
        {:ok, []}
      {:ok, _} ->
        {:error, :stale}
      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def insert(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts

    case query(repo, :insert_one, [coll, command], opts) do
      {:ok, result}   -> {:ok, result}
      {:error, error} -> check_constraint_errors(error)
    end
  end

  def insert_all(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = query.opts ++ opts

    case query(repo, :insert_many, [coll, command], opts) do
      {:ok, %{inserted_ids: ids}} ->
        {Enum.count(ids), nil}
      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def command(repo, %CommandQuery{} = query, opts) do
    command  = query.command
    opts     = query.opts ++ opts

    query(repo, :command!, [command], opts)
  end

  defp query(repo, operation, args, opts) do
    {conn, default_opts} = repo.__pool__
    args = [conn] ++ args ++ [with_log(repo, opts ++ default_opts)]
    apply(Mongo, operation, args)
  end

  defp with_log(repo, opts) do
    case Keyword.pop(opts, :log, true) do
      {true, opts}  -> [log: &log(repo, &1)] ++ opts
      {false, opts} -> opts
    end
  end

  defp log(repo, entry) do
    %{connection_time: query_time, decode_time: decode_time,
      pool_time: queue_time, result: result, query: query, params: params} = entry
    repo.__log__(%Ecto.LogEntry{query_time: query_time, decode_time: decode_time,
                                queue_time: queue_time, result: log_result(result),
                                params: [], query: &format_query(&1, query, params)})
  end

  defp log_result({:ok, _query, res}), do: {:ok, res}
  defp log_result(other), do: other

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

  alias Mongo.Query

  # TODO: fix logging
  defp format_query(_entry, %Query{action: :command}, [command]) do
    ["COMMAND " | inspect(command)]
  end
  defp format_query(_entry, :insert_one, [coll, doc, _opts]) do
    ["INSERT", format_part("coll", coll), format_part("document", doc)]
  end
  defp format_query(_entry, :insert_many, [coll, docs, _opts]) do
    ["INSERT", format_part("coll", coll), format_part("documents", docs)]
  end
  defp format_query(_entry, :delete_one, [coll, filter, _opts]) do
    ["DELETE", format_part("coll", coll), format_part("filter", filter),
     format_part("many", false)]
  end
  defp format_query(_entry, :delete_many, [coll, filter, _opts]) do
    ["DELETE", format_part("coll", coll), format_part("filter", filter),
     format_part("many", true)]
  end
  defp format_query(_entry, :replace_one, [coll, filter, doc, _opts]) do
    ["REPLACE", format_part("coll", coll), format_part("filter", filter),
     format_part("document", doc)]
  end
  defp format_query(_entry, :update_one, [coll, filter, update, _opts]) do
    ["UPDATE", format_part("coll", coll), format_part("filter", filter),
     format_part("update", update), format_part("many", false)]
  end
  defp format_query(_entry, :update_many, [coll, filter, update, _opts]) do
    ["UPDATE", format_part("coll", coll), format_part("filter", filter),
     format_part("update", update), format_part("many", true)]
  end
  defp format_query(_entry, :find, [coll, query, projection, _opts]) do
    ["FIND", format_part("coll", coll), format_part("query", query),
     format_part("projection", projection)]
  end
  defp format_query(_entry, :find_rest, [coll, cursor, _opts]) do
    ["GET_MORE", format_part("coll", coll), format_part("cursor_id", cursor)]
  end
  defp format_query(_entry, :kill_cursors, [cursors, _opts]) do
    ["KILL_CURSORS", format_part("cursor_ids", cursors)]
  end

  defp format_part(name, value) do
    [" ", name, "=" | inspect(value)]
  end
end
