defmodule Mongo.Ecto do
  @moduledoc """
  Ecto integration with MongoDB.

  This document will present a general overview of using Mongo with Ecto,
  including common pitfalls and extra functionalities.

  Check the [Ecto documentation](http://hexdocs.pm/ecto) for an introduction
  or [examples/simple](https://github.com/michalmuskala/mongodb_ecto/tree/master/examples/simple)
  for a sample application using Ecto and MongoDB.

  ## Repositories

  The first step to use MongoDB with Ecto is to define a repository
  with `Mongo.Ecto` as an adapter. First define a module:

      defmodule Repo do
        use Ecto.Repo, otp_app: :my_app
      end

  Then configure it your application environment, usually in your
  `config/config.exs`:

      config :my_app, Repo,
        adapter: Mongo.Ecto,
        database: "ecto_simple",
        username: "mongodb",
        password: "mongodb",
        hostname: "localhost"

  Each repository in Ecto defines a `start_link/0` function that needs to
  be invoked before using the repository. This function is generally from
  your supervision tree:

      def start(_type, _args) do
        import Supervisor.Spec

        children = [
          worker(Repo, [])
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  ## Models

  With the repository defined, we can define our models:

      defmodule Weather do
        use Ecto.Model

        # see the note below for explanation of that line
        @primary_key {:id, :binary_id, autogenerate: true}

        # weather is the MongoDB collection name
        schema "weather" do
          field :city,    :string
          field :temp_lo, :integer
          field :temp_hi, :integer
          field :prcp,    :float, default: 0.0
        end
      end

  Ecto defaults to using `:id` type for primary keys, that is translated to
  `:integer` for SQL databases, and is not handled by MongoDB. You need to
  specify the primary key to use the `:binary_id` type, that the adapter will
  translate it to ObjectID. Remember to place this declaration before the
  `schema` call.

  The name of the primary key is just a convenience, as MongoDB forces us to
  use `_id`. Every other name will be recursively changed to `_id` in all calls
  to the adapter. We propose to use `id` or `_id` as your primary key name
  to limit eventual confusion, but you are free to use whatever you like.
  Using the `autogenerate: true` option will tell the adapter to take care of
  generating new ObjectIDs. Otherwise you need to do this yourself.

  Since setting `@primary_key` for every model can be too repetitive, we
  recommend you to define your own module that properly configures it:

      defmodule MyApp.Model do
        defmacro __using__(_) do
          quote do
            use Ecto.Model
            @primary_key {:id, :binary_id, autogenerate: true}
            @foreign_key_type :binary_id # For associations
          end
        end
      end

  Now, instead of `use Ecto.Model`, you can `use MyApp.Model` in your
  modules. All Ecto types, except `:decimal`, are supported by `Mongo.Ecto`.

  By defining a schema, Ecto automatically defines a struct with
  the schema fields:

      iex> weather = %Weather{temp_lo: 30}
      iex> weather.temp_lo
      30

  The schema also allows the model to interact with a repository:

      iex> weather = %Weather{temp_lo: 0, temp_hi: 23}
      iex> Repo.insert!(weather)
      %Weather{...}

  After persisting `weather` to the database, it will return a new copy of
  `%Weather{}` with the primary key (the `id`) set. We can use this value
  to read a struct back from the repository:

      # Get the struct back
      iex> weather = Repo.get Weather, "507f191e810c19729de860ea"
      %Weather{id: "507f191e810c19729de860ea", ...}

      # Update it
      iex> weather = %{weather | temp_lo: 10}
      iex> Repo.update!(weather)
      %Weather{...}

      # Delete it
      iex> Repo.delete!(weather)
      %Weather{...}

  ## Queries

  `Mongo.Ecto` also supports writing queries in Elixir to interact with
  your MongoDB. Let's see an example:

      import Ecto.Query, only: [from: 2]

      query = from w in Weather,
            where: w.prcp > 0 or is_nil(w.prcp),
           select: w

      # Returns %Weather{} structs matching the query
      Repo.all(query)

  Queries are defined and extended with the `from` macro. The supported
  keywords in MongoDB are:

    * `:where`
    * `:order_by`
    * `:offset`
    * `:limit`
    * `:select`
    * `:preload`

  When writing a query, you are inside Ecto's query syntax. In order to
  access params values or invoke functions, you need to use the `^`
  operator, which is overloaded by Ecto:

      def min_prcp(min) do
        from w in Weather, where: w.prcp > ^min or is_nil(w.prcp)
      end

  Besides `Repo.all/1`, which returns all entries, repositories also
  provide `Repo.one/1`, which returns one entry or nil, and `Repo.one!/1`
  which returns one entry or raises.

  Please note that not all Ecto queries are valid MongoDB queries. The adapter
  will raise `Ecto.QueryError` if it encounters one, and will try to be as
  specific as possible as to what exactly is causing the problem.

  For things that are not possible to express with Elixir's syntax in queries,
  you can use keyword fragments:

      from p in Post, where: fragment("$exists": "name"), select: p

  To ease using more advanced queries, there is `Mongo.Ecto.Helpers` module
  you could import into modules dealing with queries. Currently it defines two
  functions:

    * `javascript/2` to use inline JavaScript

          from p in Post, where: ^javascript("this.visits === count", count: 10)

    * `regex/2` to use regex objects

          from p in Post, where: fragment(title: ^regex("elixir", "i"))

  Please see the documentation of the `Mongo.Ecto.Helpers` module for more
  information and supported options.

  ### Options for reader functions (`Repo.all/2`, `Repo.one/2`, etc)

  Such functions also accept options when invoked which allow
  you to use parameters specific to MongoDB `find` function:

    * `:slave_ok` - the read operation may run on secondary replica set member
    * `:partial` - partial data from a query against a sharded cluster in which
      some shards do not respond will be returned in stead of raising error

  ## Commands

  MongoDB has many administrative commands you can use to manage your database.
  We support them thourgh `Mongo.Ecto.command/2` function.

      Mongo.Ecto.command(MyRepo, createUser: "ecto", ...)

  We also support one higher level command - `Mongo.Ecto.truncate/1` that is
  used to clear the database, i.e. during testing.

      Mongo.Ecto.truncate(MyRepo)

  You can use it in your `setup` call for cleaning the database before every
  test. You can define your own module to use instead of `ExUnit.Case`, so you
  don't have to define this each time.

      defmodule MyApp.Case do
        use ExUnit.CaseTemplate

        setup do
          Mongo.Ecto.truncate(MyRepo)
          :ok
        end
      end

  Please see documentation for those functions for more information.

  ## Associations

  Ecto supports defining associations on schemas:

      defmodule Post do
        use Ecto.Model

        @primary_key {:id, :binary_id, autogenerate: true}
        @foreign_key_type :binary_id

        schema "posts" do
          has_many :comments, Comment
        end
      end

  Keep in mind that Ecto associations are stored in different Mongo
  collections and multiple queries may be required for retriving them.

  While `Mongo.Ecto` supports almost all association features in Ecto,
  keep in mind that MongoDB does not support joins as used in SQL - it's
  not possible to query your associations together with the main model.

  Some more elaborate association schemas may force Ecto to use joins in
  some queries, that are not supported by MongoDB as well. One such call
  is `Ecto.Model.assoc/2` function with a `has_many :through` association.

  You can find more information about defining associations and each respective
  association module in `Ecto.Schema` docs.

  ## Migrations

  TODO: Actually support migrations

  Ecto supports database migrations. You can generate a migration with:

      $ mix ecto.gen.migration create_posts

  This will create a new file inside `priv/repo/migrations` with the `up` and
  `down` functions. Check `Ecto.Migration` for more information.

  Keep in mind that MongoDB does not support (or need) database schemas, so
  majority of the functionality provided by `Ecto.Migration` is not useful when
  working with MongoDB. What is very usefull are index definitions.

  ## MongoDB adapter features

  The adapter uses `mongodb` for communicating with the database and a pooling
  library such as `poolboy` for managing connections.

  The adapter has:

    * Support for documents with ObjectID as their primary key
    * Support for insert, find, update and remove mongo functions
    * Support for management commands with `command/2`
    * Support for embedded objects with `:map` and `{:array, :map}` types

  ### MongoDB adapter options

  Options passed to the adapter are split into different categories decscribed
  below. All options should be given via the repository configuration.

  ### Compile time options

  Those options should be set in the config file and require
  recompilation in order to make an effect.

    * `:adapter` - The adapter name, in this case, `Mongo.Ecto`
    * `:pool` - The connection pool module, defaults to `Ecto.Adapters.Poolboy`
    * `:timeout` - The default timeout to use on queries, defaults to `5000`
    * `:log_level` - The level to use when logging queries (default: `:debug`)

  ### Connection options

    * `:hostname` - Server hostname (default: `localhost`)
    * `:port` - Server port (default: `27017`)
    * `:username` - Username
    * `:password` - User password
    * `:connect_timeout` - The timeout for establishing new connections (default: 5000)
    * `:w` - MongoDB's write convern (default: 1). If set to 0, some of the
      Ecto's functions may not work properely
    * `:j`, `:fsync`, `:wtimeout` - Other MongoDV's write concern options. Pleas
      consult MongoDB's documentation

  ### Pool options

  All pools should support the following options and can support other options,
  see `Ecto.Adapters.Poolboy`.

    * `:size` - The number of connections to keep in the pool (default: 10)
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

  @doc false
  def all(repo, query, params, opts) do
    normalized = NormalizedQuery.all(query, params)

    query(repo, :all, normalized, opts)
    |> Enum.map(&process_document(&1, normalized, id_types(repo)))
  end

  @doc false
  def update_all(repo, query, params, opts) do
    normalized = NormalizedQuery.update_all(query, params)

    {query(repo, :update_all, normalized, opts), nil}
  end

  @doc false
  def delete_all(repo, query, params, opts) do
    normalized = NormalizedQuery.delete_all(query, params)

    {query(repo, :delete_all, normalized, opts), nil}
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

  defp query(repo, fun, query, opts) do
    {pool_mod, pool, timeout} = repo.__pool__
    opts    = Keyword.put_new(opts, :timeout, timeout)
    timeout = Keyword.fetch!(opts, :timeout)
    log?    = Keyword.get(opts, :log, true)

    case Pool.run(pool_mod, pool, timeout, &query(&1, &2, fun, query, log?, opts)) do
      {:ok, {result, entry}} ->
        log(repo, entry)
        result(result)
      {:error, :noconnect} ->
        # :noconnect can never be the reason a call fails because
        # it is converted to {:nodedown, node}. This means the exit
        # reason can be easily identified.
        exit({:noconnect, {__MODULE__, :query, [repo, fun, query, opts]}})
      {:error, :noproc} ->
        raise ArgumentError, "repo #{inspect repo} is not started, " <>
                             "please ensure it is part of your supervision tree"
    end
  end

  defp query({mod, conn}, _queue_time, fun, query, false, opts) do
    {apply(mod, fun, [conn, query, opts]), nil}
  end
  defp query({mod, conn}, queue_time, fun, query, true, opts) do
    {query_time, res} = :timer.tc(mod, fun, [conn, query, opts])

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

  ## Usage

      Mongo.Ecto.command(Repo, drop: "collection")

  For list of available commands plese see: http://docs.mongodb.org/manual/reference/command/
  """
  def command(repo, command, opts \\ [])
  def command(repo, command, opts) when is_atom(repo) do
    with_new_conn(repo.config, &command(&1, command, opts))
  end
  def command(conn, command, opts) when is_pid(conn) do
    Connection.command(conn, command, opts)
    |> result
  end

  defp list_collections(conn) when is_pid(conn) do
    query = %ReadQuery{coll: "system.namespaces"}

    conn
    |> Connection.all(query)
    |> result
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
    command(conn, drop: collection)
  end
end
