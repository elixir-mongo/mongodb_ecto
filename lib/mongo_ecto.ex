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

  alias Mongo.Ecto.Query
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
  def all(repo, query, params, _opts) do
    {collection, selector, projector, skip, return, fields, pk} = Query.all(query, params)

    from = query.from
    id_types = id_types(repo)
    params = List.to_tuple(params)
    opts = [num_return: return, num_skip: skip]

    with_conn(repo, fn module, conn ->
      module.all(conn, collection, selector, projector, opts)
    end)
    |> Enum.map(&process_document(&1, fields, from, id_types, pk, params))
  end

  @doc false
  def update_all(repo, query, values, params, _opts) do
    {collection, selector, command} = Query.update_all(query, values, params)

    with_conn(repo, fn module, conn ->
      module.update_all(conn, collection, selector, command)
    end)
  end

  @doc false
  def delete_all(repo, query, params, _opts) do
    {collection, selector} = Query.delete_all(query, params)

    with_conn(repo, fn module, conn ->
      module.delete_all(conn, collection, selector)
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

  def insert(repo, source, params, nil, [], _opts) do
    do_insert(repo, source, params, nil)
    {:ok, []}
  end

  def insert(repo, source, params, {pk, :binary_id, nil}, [], _opts) do
    %BSON.ObjectId{value: value} = id = Mongo.IdServer.new
    do_insert(repo, source, [{pk, id} | params], pk)

    {:ok, [{pk, value}]}
  end

  def insert(repo, source, params, {pk, :binary_id, _value}, [], _opts) do
    do_insert(repo, source, params, pk)
    {:ok, []}
  end

  @doc false
  def update(_repo, source, _fields, _filter, {key, :id, _}, _returning, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def update(_repo, source, _fields, _filter, _autogen, [_] = returning, _opts) do
    raise ArgumentError,
      "MongoDB adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect source} are tagged as such: #{inspect returning}"
  end

  def update(repo, source, fields, filter, {pk, :binary_id, _value}, [], _opts) do
    {collection, selector, command} = Query.update(source, fields, filter, pk)

    with_conn(repo, fn module, conn ->
      module.update(conn, collection, selector, command)
    end)
    # TODO handle response
    {:ok, []}
  end

  @doc false
  def delete(_repo, source, _filter, {key, :id, _}, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def delete(repo, source, filter, {pk, :binary_id, _value}, _opts) do
    {collection, selector} = Query.delete(source, filter, pk)

    with_conn(repo, fn module, conn ->
      module.delete(conn, collection, selector)
    end)
    # TODO handle response
    {:ok, []}
  end

  defp do_insert(repo, source, params, pk) do
    {collection, document} = Query.insert(source, params, pk)

    with_conn(repo, fn module, conn ->
      module.insert(conn, collection, document)
    end)
  end

  defp process_document(document, fields, {source, model}, id_types, pk, params) do
    document = Query.decode_document(document, pk)

    Enum.map(fields, fn
      {:&, _, [0]} ->
        row = model.__schema__(:fields)
              |> Enum.map(&Map.get(document, Atom.to_string(&1), nil))
              |> List.to_tuple
        model.__schema__(:load, source, 0, row, id_types)
      {{:., _, [{:&, _, [0]}, field]}, _, []} ->
        Map.get(document, Atom.to_string(field))
      value ->
        value
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
    result = fun.(conn)
    :ok = Connection.disconnect(conn)

    result
  end

  ## Mongo specific calls

  @doc """
  Returns a list of all collections in the database and their options

  When available uses `listCollections` command, otherwise queries the
  `system.namespaces` collection and sanitizes output to match that of
  the `listCollections` command.

  For more information see: http://docs.mongodb.org/manual/reference/command/listCollections/
  """
  def list_collections(repo) when is_atom(repo) do
    with_new_conn(repo.config, &list_collections/1)
  end
  def list_collections(conn) when is_pid(conn) do
    # if Version.match?(server_version(conn), ">3.0.0") do
    #   {:ok, collections} = command(conn, %{listCollections: 1})

    #   collections
    # else
    Connection.all(conn, "system.namespaces", %{}, %{})
    |> sanitize_old_collections
    # end
  end

  defp sanitize_old_collections(collections) do
    collections
    |> Enum.reject(&special_collection?/1)
    |> Enum.map(fn collection ->
      update_in(collection, ["name"], fn name ->
        [_, name] = String.split(name, ".", parts: 2)
        name
      end)
      |> Map.put_new("options", %{})
    end)
  end

  defp special_collection?(map) do
    map |> Map.get("name") |> String.contains?("$")
  end

  @doc """
  Return mongod version.

  Uses `buildInfo` command to retrieve information.
  """
  def server_version(repo) when is_atom(repo) do
    with_new_conn(repo.config, &server_version/1)
  end
  def server_version(conn) when is_pid(conn) do
    {:ok, [build_info]} = command(conn, %{buildInfo: 1})

    Map.get(build_info, "version")
  end

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
    |> Enum.map(&Map.fetch!(&1, "name"))
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

  defp drop_collection(conn, collection) when is_pid(conn) do
    command(conn, %{drop: collection})
  end

  @doc """
  Runs a command in the database.

  For list of available commands plese see: http://docs.mongodb.org/manual/reference/command/
  """
  def command(repo, command) when is_atom(repo) do
    with_new_conn(repo.config, &command(&1, command))
  end
  def command(conn, command) when is_pid(conn) do
    Connection.command(conn, command)
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
