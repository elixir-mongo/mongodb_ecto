defmodule Mongo.Ecto do
  @moduledoc """
  Adapter module for MongoDB

  It uses `mongo` for communicating with the database and manages
  a connection pool using `poolboy`.

  ## Features

    * Support for documents with ObjectID as their primary key
    * Support for insert, find, update and delete mongo functions
    * Support for management commands with `command/2`
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  alias Mongo.Ecto.NormalizedQuery
  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.Decoder
  alias Mongo.Ecto.ObjectID
  alias Mongo.Ecto.Connection

  alias Ecto.Adapters.Pool

  ## Adapter

  @doc false
  defmacro __before_compile__(env) do
    config =
      env.module
      |> Module.get_attribute(:config)

    timeout = Keyword.get(config, :timeout, 5000)
    pool_mod = Keyword.get(config, :pool, Ecto.Adapters.Poolboy)

    quote do
      def __pool__ do
        {unquote(pool_mod), __MODULE__.Pool, unquote(timeout)}
      end
    end
  end

  @doc false
  def start_link(repo, opts) do
    {:ok, _} = Application.ensure_all_started(:mongodb_ecto)

    {pool_mod, pool, _} = repo.__pool__
    opts = opts
      |> Keyword.put(:timeout, Keyword.get(opts, :connect_timeout, 5000))
      |> Keyword.put(:name, pool)
      |> Keyword.put_new(:size, 10)

    pool_mod.start_link(Connection, opts)
  end

  @doc false
  def stop(repo) do
    {pool_mod, pool, _} = repo.__pool__
    pool_mod.stop(pool)
  end

  @doc false
  def id_types(_repo) do
    %{binary_id: ObjectID}
  end

  defp query(repo, fun, query, opts) do
    {pool_mod, pool, timeout} = repo.__pool__
    opts    = Keyword.put_new(opts, :timeout, timeout)
    timeout = Keyword.fetch!(opts, :timeout)
    log?    = Keyword.get(opts, :log, true)

    query_fun = fn(ref, _mode, _depth, queue_time) ->
      query(ref, queue_time, fun, query, log?, timeout, opts)
    end

    case Pool.run(pool_mod, pool, timeout, query_fun) do
      {:ok, {result, entry}} ->
        log(repo, entry)
        result(result)
      {:ok, :noconnect} ->
        # :noconnect can never be the reason a call fails because
        # it is converted to {:nodedown, node}. This means the exit
        # reason can be easily identified.
        exit({:noconnect, {__MODULE__, :query, [repo, fun, query, opts]}})
      {:error, :noproc} ->
        raise ArgumentError, "repo #{inspect repo} is not started, " <>
                             "please ensure it is part of your supervision tree"
    end
  end

  defp query(ref, queue_time, fun, query, log?, timeout, opts) do
    case Pool.connection(ref) do
      {:ok, {mod, conn}} ->
        query(ref, mod, conn, queue_time, fun, query, log?, timeout, opts)
      {:error, :noconnect} ->
        :noconnect
    end
  end

  defp query(ref, mod, conn, _queue_time, fun, query, false, timeout, opts) do
    query_fun = fn -> apply(mod, fun, [conn, query, opts]) end

    {Pool.fuse(ref, timeout, query_fun), nil}
  end
  defp query(ref, mod, conn, queue_time, fun, query, true, timeout, opts) do
    query_fun = fn -> :timer.tc(mod, fun, [conn, query, opts]) end

    {query_time, res} = Pool.fuse(ref, timeout, query_fun)
    entry = %Ecto.LogEntry{query: &format_query(&1, fun, query), params: [],
                           result: res, query_time: query_time, queue_time: queue_time}

    {res, entry}
  end

  defp log(_repo, nil), do: :ok
  defp log(repo, entry), do: repo.log(entry)

  defp result({:ok, result}),
    do: result
  defp result({:error, %{__exception__: _} = error}),
    do: raise error
  defp result({:error, reason}),
    do: raise(ArgumentError, "MongoDB driver errored with #{inspect reason}")

  defp format_query(_entry, :all, query) do
    ["FIND coll=", inspect(query.coll),
     " query=", inspect(query.query),
     " projection=", inspect(query.projection)]
  end
  defp format_query(_entry, :insert, query) do
    ["INSERT coll=", inspect(query.coll),
     " document=", inspect(query.command)]
  end
  defp format_query(_entry, op, query) when op in [:delete, :delete_all] do
    ["REMOVE coll=", inspect(query.coll),
     " query=", inspect(query.query),
     " opts=", inspect(query.opts)]
  end
  defp format_query(_entry, op, query) when op in [:update, :update_all] do
    ["UPDATE coll=", inspect(query.coll),
     " query=", inspect(query.query),
     " command=", inspect(query.command),
     " opts=", inspect(query.opts)]
  end

  @doc false
  def all(repo, query, params, opts) do
    normalized = NormalizedQuery.all(query, params)

    query(repo, :all, normalized, opts)
    |> Enum.map(&process_document(&1, normalized, id_types(repo)))
  end

  @doc false
  def update_all(repo, query, values, params, opts) do
    normalized = NormalizedQuery.update_all(query, values, params)

    query(repo, :update_all, normalized, opts)
  end

  @doc false
  def delete_all(repo, query, params, opts) do
    normalized = NormalizedQuery.delete_all(query, params)

    query(repo, :delete_all, normalized, opts)
  end

  @doc false
  def insert(_repo, source, _params, {key, :id, _}, _returning, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def insert(_repo, source, _params, _autogen, [_] = returning, _opts) do
    raise ArgumentError,
      "MongoDB adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect source} are tagged as such: #{inspect returning}"
  end

  def insert(repo, source, params, nil, [], opts) do
    normalized = NormalizedQuery.insert(source, params, nil)

    query(repo, :insert, normalized, opts)
    |> single_result([])
  end

  def insert(repo, source, params, {pk, :binary_id, nil}, [], opts) do
    %BSON.ObjectId{value: value} = id = Mongo.IdServer.new
    params = Keyword.put(params, pk, id)

    normalized = NormalizedQuery.insert(source, params, pk)

    query(repo, :insert, normalized, opts)
    |> single_result([{pk, value}])
  end

  def insert(repo, source, params, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.insert(source, params, pk)

    query(repo, :insert, normalized, opts)
    |> single_result([])
  end

  @doc false
  def update(_repo, source, _fields, _filter, {key, :id, _}, _returning, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def update(_repo, source, _fields, _filter, _autogen, [_|_] = returning, _opts) do
    raise ArgumentError,
      "MongoDB adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect source} are tagged as such: #{inspect returning}"
  end

  def update(repo, source, fields, filter, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.update(source, fields, filter, pk)

    query(repo, :update, normalized, opts)
    |> single_result([])
  end

  @doc false
  def delete(_repo, source, _filter, {key, :id, _}, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def delete(repo, source, filter, {pk, :binary_id, _value}, opts) do
    normalized = NormalizedQuery.delete(source, filter, pk)

    query(repo, :delete, normalized, opts)
    |> single_result([])
  end

  defp single_result(1, result), do: {:ok, result}
  defp single_result(_, _),      do: {:error, :stale}

  defp process_document(document, %ReadQuery{fields: fields, pk: pk}, id_types) do
    document = Decoder.decode_document(document, pk)

    Enum.map(fields, fn
      {:document, nil} ->
        document
      {:model, {model, coll}} ->
        row = model.__schema__(:fields)
              |> Enum.map(&Map.get(document, Atom.to_string(&1)))
              |> List.to_tuple
        model.__schema__(:load, coll, 0, row, id_types)
      {:field, field} ->
        Map.get(document, Atom.to_string(field))
      value ->
        Decoder.decode_value(value, pk)
    end)
  end

  ## Storage

  # Noop for MongoDB, as any databases and collections are created as needed.
  @doc false
  def storage_up(_opts) do
    :ok
  end

  @doc false
  def storage_down(opts) do
    with_new_conn(opts, fn conn ->
      command(conn, %{dropDatabase: 1})
    end)
  end

  defp with_new_conn(opts, fun) do
    {:ok, conn} = Connection.connect(opts)
    try do
      fun.(conn)
    after
      :ok = Connection.disconnect(conn)
    end
  end

  ## Mongo specific calls

  @doc """
  Drops all the collections in current database except system ones.

  Especially usefull in testing.
  """
  def truncate(repo) when is_atom(repo) do
    with_new_conn(repo.config, &truncate/1)
  end
  def truncate(conn) when is_pid(conn) do
    conn
    |> list_collections
    |> Enum.reject(&String.starts_with?(&1, "system"))
    |> Enum.flat_map_reduce(:ok, fn collection, :ok ->
      case drop_collection(conn, collection) do
        {:error, _} = error -> {:halt, error}
        _                   -> {[collection], :ok}
      end
    end)
    |> case do
         {dropped, :ok} -> {:ok, dropped}
         {dropped, {:error, reason}} -> {:error, {reason, dropped}}
       end
  end

  @doc """
  Runs a command in the database.

  For list of available commands plese see: http://docs.mongodb.org/manual/reference/command/
  """
  def command(repo, command, opts \\ [])
  def command(repo, command, opts) when is_atom(repo) do
    with_new_conn(repo.config, &command(&1, command, opts))
  end
  def command(conn, command, opts) when is_pid(conn) do
    Connection.command(conn, command, opts)
  end

  defp list_collections(conn) when is_pid(conn) do
    query = %ReadQuery{coll: "system.namespaces"}

    {:ok, collections} = Connection.all(conn, query)

    collections
    |> Enum.map(&Map.fetch!(&1, "name"))
    |> Enum.map(fn collection ->
      collection |> String.split(".", parts: 2) |> Enum.at(1)
    end)
    |> Enum.reject(fn
      "system" <> _rest -> true
      collection        -> String.contains?(collection, "$")
    end)
  end

  defp drop_collection(conn, collection) when is_pid(conn) do
    command(conn, %{drop: collection})
  end
end
