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
  alias NormalizedQuery.ReadQuery
  alias Mongo.Ecto.Decoder
  alias Mongo.Ecto.ObjectID
  alias Mongo.Ecto.Connection
  alias Ecto.Adapters.Worker

  ## Adapter

  @doc false
  defmacro __before_compile__(env) do
    timeout =
      env.module
      |> Module.get_attribute(:config)
      |> Keyword.get(:timeout, 5000)

    quote do
      def __pool__ do
        {__MODULE__.Pool, unquote(timeout)}
      end
    end
  end

  @doc false
  def start_link(repo, opts) do
    {:ok, _} = Application.ensure_all_started(:mongodb_ecto)
    {pool_opts, worker_opts} = split_opts(repo, opts)
    :poolboy.start_link(pool_opts, {Connection, worker_opts})
  end

  @doc false
  def stop(repo) do
    repo.__pool__ |> elem(0) |> :poolboy.stop
  end

  @doc false
  def id_types(_repo) do
    %{binary_id: ObjectID}
  end

  defp with_conn(repo, fun) do
    {pool, timeout} = repo.__pool__

    worker = :poolboy.checkout(pool, true, timeout)
    try do
      {module, conn} = Worker.ask!(worker, timeout)
      fun.(module, conn)
    after
      :ok = :poolboy.checkin(pool, worker)
    end
  end

  @doc false
  def all(repo, query, params, opts) do
    normalized = NormalizedQuery.all(query, params)

    with_conn(repo, fn module, conn ->
      module.all(conn, normalized, opts)
    end)
    |> elem(1)
    |> Enum.map(&process_document(&1, normalized, id_types(repo)))
  end

  @doc false
  def update_all(repo, query, values, params, opts) do
    normalized = NormalizedQuery.update_all(query, values, params)

    with_conn(repo, fn module, conn ->
      module.update_all(conn, normalized, opts)
    end)
  end

  @doc false
  def delete_all(repo, query, params, opts) do
    normalized = NormalizedQuery.delete_all(query, params)

    with_conn(repo, fn module, conn ->
      module.delete_all(conn, normalized, opts)
    end)
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
    do_insert(repo, source, params, nil, opts)
  end

  def insert(repo, source, params, {pk, :binary_id, nil}, [], opts) do
    %BSON.ObjectId{value: value} = id = Mongo.IdServer.new
    case do_insert(repo, source, [{pk, id} | params], pk, opts) do
      {:ok, []} -> {:ok, [{pk, value}]}
      other     -> other
    end
  end

  def insert(repo, source, params, {pk, :binary_id, _value}, [], opts) do
    do_insert(repo, source, params, pk, opts)
  end

  defp do_insert(repo, coll, document, pk, opts) do
    normalized = NormalizedQuery.insert(coll, document, pk)

    with_conn(repo, fn module, conn ->
      module.insert(conn, normalized, opts)
    end)
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

    with_conn(repo, fn module, conn ->
      module.update(conn, normalized, opts)
    end)
  end

  @doc false
  def delete(_repo, source, _filter, {key, :id, _}, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def delete(repo, source, filter, {pk, :binary_id, _value}, opts) do
    normalized = NormalizedQuery.delete(source, filter, pk)

    with_conn(repo, fn module, conn ->
      module.delete(conn, normalized, opts)
    end)
  end

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
        {:ok, _} -> {[collection], :ok}
        error    -> {[], error}
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

  defp split_opts(repo, opts) do
    {pool_name, _} = repo.__pool__

    {pool_opts, worker_opts} = Keyword.split(opts, [:size, :max_overflow])

    pool_opts = pool_opts
      |> Keyword.put_new(:size, 10)
      |> Keyword.put_new(:max_overflow, 0)
      |> Keyword.put(:worker_module, Worker)
      |> Keyword.put(:name, {:local, pool_name})

    worker_opts = worker_opts
      |> Keyword.put(:timeout, Keyword.get(worker_opts, :connect_timeout, 5000))

    {pool_opts, worker_opts}
  end
end
