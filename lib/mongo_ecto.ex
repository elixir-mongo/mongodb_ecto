defmodule Mongo.Ecto do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  alias Mongo.Ecto.Bson
  alias Mongo.Ecto.Query
  alias Mongo.Ecto.ObjectID
  alias Mongo.Ecto.Connection
  alias Ecto.Adapters.Worker

  ## Adapter

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

  def start_link(repo, opts) do
    {pool_opts, worker_opts} = split_opts(repo, opts)

    :poolboy.start_link(pool_opts, {Connection, worker_opts})
  end

  def stop(repo) do
    repo.__pool__ |> elem(0) |> :poolboy.stop
  end

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

  def all(repo, query, params, _opts) do
    {collection, model, selector, projector, skip, batch_size} = Query.all(query, params)
    pk = primary_key(model)

    selector = Bson.to_bson(selector, pk)
    projector = Bson.to_bson(projector, pk)

    fields = query.select.fields
    from = query.from
    id_types = id_types(repo)
    params = List.to_tuple(params)

    with_conn(repo, fn module, conn ->
      module.all(conn, collection, selector, projector, skip, batch_size)
    end)
    |> Enum.map(&Bson.from_bson(&1, pk))
    |> Enum.map(&process_document(&1, fields, from, id_types, params))
  end


  def update_all(repo, query, values, params, _opts) do
    {collection, model, selector, command} = Query.update_all(query, values, params)
    pk = primary_key(model)

    selector = Bson.to_bson(selector, pk)
    command  = Bson.to_bson(command, pk)

    with_conn(repo, fn module, conn ->
      module.update_all(conn, collection, selector, command)
    end)
  end

  def delete_all(repo, query, params, _opts) do
    {collection, model, selector} = Query.delete_all(query, params)
    pk = primary_key(model)

    selector = Bson.to_bson(selector, pk)

    with_conn(repo, fn module, conn ->
      module.delete_all(conn, collection, selector)
    end)
  end

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
    result = do_insert(repo, source, params, pk) |> Bson.from_bson(pk)

    {:ok, [{pk, Map.fetch!(result, pk)}]}
  end

  def insert(repo, source, params, {pk, :binary_id, _value}, [], _opts) do
    do_insert(repo, source, params, pk)
    {:ok, []}
  end

  def update(_repo, source, _fields, _filter, _autogen, [_] = returning, _opts) do
    raise ArgumentError,
      "MongoDB adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect source} are tagged as such: #{inspect returning}"
  end

  def update(repo, source, fields, filter, {pk, :binary_id, _value}, [], _opts) do
    {collection, selector, command} = Query.update(source, fields, filter)

    selector = Bson.to_bson(selector, pk)
    command  = Bson.to_bson(command, pk)

    with_conn(repo, fn module, conn ->
      module.update(conn, collection, selector, command)
    end)

    {:ok, []}
  end

  def delete(repo, source, filter, {pk, :binary_id, _value}, _opts) do
    {collection, selector} = Query.delete(source, filter)

    selector = Bson.to_bson(selector, pk)

    with_conn(repo, fn module, conn ->
      module.delete(conn, collection, selector)
    end)

    {:ok, []}
  end

  def primary_key(nil), do: nil
  def primary_key(model) do
    case model.__schema__(:primary_key) do
      [pk] -> pk
      _    -> :id
    end
  end

  defp do_insert(repo, source, params, pk) do
    document =
      params
      |> Enum.filter(fn
        {_key, %Ecto.Query.Tagged{value: nil}} -> false
        {_key, nil} -> false
        _ -> true
      end)
      |> Bson.to_bson(pk)

    with_conn(repo, fn module, conn ->
      module.insert(conn, source, document)
    end)
  end

  def process_document(document, fields, {source, model}, id_types, params) do
    Enum.map(fields, fn
      {:&, _, [0]} ->
        row = model.__schema__(:fields)
              |> Enum.map(&Map.get(document, &1, nil))
              |> List.to_tuple
        model.__schema__(:load, source, 0, row, id_types)
      {{:., _, [{:&, _, [0]}, field]}, _, []} ->
        Map.get(document, field)
      %Ecto.Query.Tagged{value: {:^, _, [idx]}} ->
        # I really don't like it, but I don't know what else to do. I cant pass
        # literal selects to the db...
        case elem(params, idx) do
          %Ecto.Query.Tagged{value: value} -> value
          value -> value
        end
      value ->
        value
    end)
  end

  ## Storage

  # Noop for MongoDB, as any databases and collections are created as needed.
  def storage_up(_opts) do
    :ok
  end

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

  def create_collection(where, collection, opts \\ [])
  def create_collection(repo, collection, opts) when is_atom(repo) do
    with_new_conn(repo.config, &create_collection(&1, collection, opts))
  end
  def create_collection(conn, collection, opts) when is_pid(conn) do
    # Order is important - create has to be first
    command = [create: collection] ++ opts

    command(conn, command)
  end

  def list_collections(repo) when is_atom(repo) do
    with_new_conn(repo.config, &list_collections/1)
  end
  def list_collections(conn) when is_pid(conn) do
    if Version.match?(server_version(conn), ">2.8.0") do
      {:ok, collections} = command(conn, %{listCollections: 1})

      collections
    else
      Connection.all(conn, "system.namespaces", {}, {}, 0, 0)
      |> Enum.map(&Bson.from_bson(&1, nil))
      |> sanitize_old_collections
    end
  end

  defp sanitize_old_collections(collections) do
    collections
    |> Enum.reject(&String.contains?(&1.name, "$"))
    |> Enum.map(fn collection ->
      update_in(collection.name, fn name ->
        [_, name] = String.split(name, ".", parts: 2)
        name
      end)
      |> Map.put_new(:options, %{})
    end)
  end

  def server_version(repo) when is_atom(repo) do
    with_new_conn(repo.config, &server_version/1)
  end
  def server_version(conn) when is_pid(conn) do
    {:ok, build_info} = command(conn, %{buildInfo: 1})

    :bson.lookup(:version, build_info) |> elem(0)
  end

  def truncate(repo) when is_atom(repo) do
    with_new_conn(repo.config, &truncate/1)
  end
  def truncate(conn) when is_pid(conn) do
    conn
    |> list_collections
    |> Enum.map(&Map.get(&1, :name))
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

  def drop_collection(repo, collection) when is_atom(repo) do
    with_new_conn(repo.config, &drop_collection(&1, collection))
  end
  def drop_collection(conn, collection) when is_pid(conn) do
    command(conn, %{drop: collection})
  end

  def command(repo, command) when is_atom(repo) do
    with_new_conn(repo.config, &command(&1, command))
  end
  def command(conn, command) when is_pid(conn) do
    command = Bson.to_bson(command, nil)

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
