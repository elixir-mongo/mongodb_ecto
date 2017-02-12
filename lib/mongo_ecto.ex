defmodule Mongo.Ecto do
  @moduledoc """
  Ecto integration with MongoDB.

  This document will present a general overview of using MongoDB with Ecto,
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
  translate to ObjectID. Remember to place this declaration before the
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

  There is also support for count function in queries that uses `MongoDB`'s
  `count` command. Please not that unlike in SQL databases you can only select
  a count - there is no support for querying using a count, there is also no
  support for counting documents and selecting them at the same time.

  Please note that not all Ecto queries are valid MongoDB queries. The adapter
  will raise `Ecto.QueryError` if it encounters one, and will try to be as
  specific as possible as to what exactly is causing the problem.

  For things that are not possible to express with Elixir's syntax in queries,
  you can use keyword fragments:

      from p in Post, where: fragment(key:["$exists": true]), select: p


  To ease of using in more advanced queries, there is `Mongo.Ecto.Helpers` module
  you could import into modules dealing with queries.
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

  ## Embedded models

  Ecto supports defining relations using embedding models directly inside the
  parent model, and that fits MongoDB's design perfectly.

      defmodule Post do
        #...

        schema "posts" do
          embeds_many :comments, Comment
        end
      end

      defmodule Comment do
        embedded_schema do
          field :body, :string
        end
      end

  You can find more information about defining embedded models in the
  `Ecto.Schema` docs.

  ## Indexes and Migrations

  Although schema migrations make no sense for databases such as MongoDB
  there is one field where they can be very benefitial - indexes. Because of
  this Mongodb.Ecto supports Ecto's database migrations. You can generate a
  migration with:

      $ mix ecto.gen.migration create_posts

  This will create a new file inside `priv/repo/migrations` with the `up` and
  `down` functions. Check `Ecto.Migration` for more information.

  Because MongoDB does not support (or need) database schemas majority of the
  functionality provided by `Ecto.Migration` is not useful when working with
  MongoDB. As we've already noted the most useful part is indexing, but there
  are others - creating capped collections, executing administrative commands,
  or migrating data, e.g.:

      defmodule SampleMigration do
        use Ecto.Migration

        def up do
          create table(:my_table, options: [capped: true, size: 1024])
          create index(:my_table, [:value])
          create unique_index(:my_table, [:unique_value])
          execute touch: "my_table", data: true, index: true
        end

        def down do
          # ...
        end
      end

  MongoDB adapter does not support `create_if_not_exists` or `drop_if_exists`
  migration functions.

  ## MongoDB adapter features

  The adapter uses `mongodb` for communicating with the database and a pooling
  library such as `poolboy` for managing connections.

  The adapter has support for:

    * documents with ObjectID as their primary key
    * insert, find, update, remove and count mongo functions
    * management commands with `command/2`
    * embedded documents either with `:map` type, or embedded models
    * partial updates using `change_map/2` and `change_array/2` from the
      `Mongo.Ecto.Helpers` module
    * queries using javascript expresssion and regexes using respectively
      `javascript/2` and `regex/2` functions from `Mongo.Ecto.Helpers` module.

  ### MongoDB adapter options

  Options passed to the adapter are split into different categories decscribed
  below. All options should be given via the repository configuration.

  ### Compile time options

  Those options should be set in the config file and require
  recompilation in order to make an effect.

    * `:adapter` - The adapter name, in this case, `Mongo.Ecto`
    * `:pool` - The connection pool module, defaults to `Mongo.Pool.Poolboy`
    * `:log_level` - The level to use when logging queries (default: `:debug`)

  ### Connection options

    * `:hostname` - Server hostname (default: `localhost`)
    * `:port` - Server port (default: `27017`)
    * `:username` - Username
    * `:password` - User password
    * `:connect_timeout` - The timeout for establishing new connections (default: 5000)
    * `:w` - MongoDB's write convern (default: 1). If set to 0, some of the
      Ecto's functions may not work properely
    * `:j`, `:fsync`, `:wtimeout` - Other MongoDB's write concern options. Please
      consult MongoDB's documentation

  ### Pool options

  `Mongo.Ecto` does not use Ecto pools, instead pools provided by the MongoDB
  driver are used. The default poolboy adapter accepts following options:

    * `:pool_size` - The number of connections to keep in the pool (default: 10)
    * `:max_overflow` - The maximum overflow of connections (default: 0)

  For other adapters, please see their documentation.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Migration

  alias Mongo.Ecto.NormalizedQuery
  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery
  alias Mongo.Ecto.NormalizedQuery.CountQuery
  alias Mongo.Ecto.NormalizedQuery.AggregateQuery
  alias Mongo.Ecto.Connection
  alias Mongo.Ecto.Conversions

  ## Adapter

  @doc false
  defmacro __before_compile__(env) do
    config = Module.get_attribute(env.module, :config)
    pool   = Keyword.get(config, :pool, DBConnection.Poolboy)
    pool_name = pool_name(env.module, config)
    norm_config = normalize_config(config)
    quote do
      @doc false
      def __pool__, do: {unquote(pool_name), unquote(Macro.escape(norm_config))}

      defoverridable [__pool__: 0]
    end
  end

  @pool_timeout 5_000
  @timeout 15_000

  defp normalize_config(config) do
    config
    |> Keyword.delete(:name)
    |> Keyword.put_new(:timeout, @timeout)
    |> Keyword.put_new(:pool_timeout, @pool_timeout)
  end

  defp pool_name(module, config) do
    Keyword.get(config, :pool_name, default_pool_name(module, config))
  end

  defp default_pool_name(repo, config) do
    Module.concat(Keyword.get(config, :name, repo), Pool)
  end

  @doc false
  def application, do: :mongodb_ecto

  @doc false
  def child_spec(repo, opts) do
    # Check if the pool options should be overridden
    {pool_name, pool_opts} =
      case Keyword.fetch(opts, :pool) do
        {:ok, pool} ->
          {pool_name(repo, opts), opts}
        _ ->
          repo.__pool__
      end
    opts = [name: pool_name] ++ Keyword.delete(opts, :pool) ++ pool_opts

    Mongo.child_spec(opts)
  end

  @doc false
  def ensure_all_started(repo, type) do
    {_, opts} = repo.__pool__
    with {:ok, pool} <- DBConnection.ensure_all_started(opts, type),
         {:ok, mongo} <- Application.ensure_all_started(:mongodb, type),
      do: {:ok, pool ++ mongo}
  end

  @doc false
  def loaders(:time,      type), do: [&load_time/1, type]
  def loaders(:date,      type), do: [&load_date/1, type]
  def loaders(:datetime,  type), do: [&load_datetime/1, type]
  def loaders(:binary_id, type), do: [&load_objectid/1, type]
  def loaders(:uuid,      type), do: [&load_binary/1,   type]
  def loaders(:binary,    type), do: [&load_binary/1,   type]
  def loaders(_base,      type), do: [type]

  defp load_time(%BSON.DateTime{} = time) do
    {{_,_,_}, time} = BSON.DateTime.to_datetime(time)
    {:ok, time}
  end
  defp load_time(_),
    do: :error

  defp load_date(%BSON.DateTime{} = date) do
    {date, {_, _, _, _}} = BSON.DateTime.to_datetime(date)
    {:ok, date}
  end
  defp load_date(_),
    do: :error

  defp load_datetime(%BSON.DateTime{} = datetime),
    do: {:ok, BSON.DateTime.to_datetime(datetime)}
  defp load_datetime(_),
    do: :error

  defp load_binary(%BSON.Binary{binary: binary}),
    do: {:ok, binary}
  defp load_binary(_),
    do: :error

  defp load_objectid(%BSON.ObjectId{} = objectid) do
    try do
      {:ok, BSON.ObjectId.encode!(objectid)}
    catch
      ArgumentError ->
        :error
    end
  end
  defp load_objectid(_), do: :error

  @doc false
  def dumpers(:time,      type), do: [type, &dump_time/1]
  def dumpers(:date,      type), do: [type, &dump_date/1]
  def dumpers(:datetime,  type), do: [type, &dump_datetime/1]
  def dumpers(:binary_id, type), do: [type, &dump_objectid/1]
  def dumpers(:uuid,      type), do: [type, &dump_binary(&1, :uuid)]
  def dumpers(:binary,    type), do: [type, &dump_binary(&1, :generic)]
  def dumpers(_base,      type), do: [type]

  defp dump_time({_, _, _, _} = time),
    do: {:ok, BSON.DateTime.from_datetime({{0, 0, 0}, time})}
  defp dump_time(_),
    do: :error

  defp dump_date({_, _, _} = date),
    do: {:ok, BSON.DateTime.from_datetime({date, {0, 0, 0, 0}})}
  defp dump_date(_),
    do: :error

  defp dump_datetime({{_, _, _}, {_, _, _, _}} = datetime),
    do: {:ok, BSON.DateTime.from_datetime(datetime)}
  defp dump_datetime(_),
    do: :error

  defp dump_binary(binary, subtype) when is_binary(binary),
    do: {:ok, %BSON.Binary{binary: binary, subtype: subtype}}
  defp dump_binary(_, _),
    do: :error

  defp dump_objectid(<<objectid :: binary-size(24)>>) do
    try do
      {:ok, BSON.ObjectId.decode!(objectid)}
    catch
      ArgumentError ->
        :error
    end
  end
  defp dump_objectid(_), do: :error

  @doc false
  def autogenerate(:id),
    do: raise "MongoDB adapter does not support `:id` type as primary key"
  def autogenerate(:embed_id),
    do: BSON.ObjectId.encode!(Mongo.object_id)
  def autogenerate(:binary_id),
    do: Mongo.object_id

  @doc false
  def prepare(function, query) do
    {:nocache, {function, query}}
  end

  @read_queries [ReadQuery, CountQuery, AggregateQuery]

  @doc false
  def execute(repo, _meta, {:nocache, {function, query}}, params, process, opts) do
    case apply(NormalizedQuery, function, [query, params]) do
      %{__struct__: read} = query when read in @read_queries ->
        {rows, count} =
          Connection.read(repo, query, opts)
          |> Enum.map_reduce(0, &{process_document(&1, query, process), &2 + 1})
        {count, rows}
      %WriteQuery{} = write ->
        result = apply(Connection, function, [repo, write, opts])
        {result, nil}
    end
  end

  @doc false
  def insert(_repo, meta, _params, [_|_] = returning, _opts) do
    raise ArgumentError,
      "MongoDB adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect meta.schema} are tagged as such: #{inspect returning}"
  end

  def insert(repo, meta, params, [], opts) do
    normalized = NormalizedQuery.insert(meta, params)

    case Connection.insert(repo, normalized, opts) do
      {:ok, _} ->
        {:ok, []}
      other ->
        other
    end
  end

  def insert_all(repo, meta, fields, params, returning, opts) do
    normalized = NormalizedQuery.insert(meta, params)

    case Connection.insert_all(repo, normalized, opts) do
      {:ok, _} ->
        {:ok, []}
      other ->
        other
    end
  end

  @doc false
  def update(repo, meta, fields, filters, returning, opts) do
    normalized = NormalizedQuery.update(meta, fields, filters)

    Connection.update(repo, normalized, opts)
  end

  @doc false
  def delete(repo, meta, filter, opts) do
    normalized = NormalizedQuery.delete(meta, filter)

    Connection.delete(repo, normalized, opts)
  end

  defp process_document(document, %{fields: fields, pk: pk}, preprocess) do
    document = Conversions.to_ecto_pk(document, pk)

    Enum.map(fields, fn
      {:field, name, field} ->
        preprocess.(field, Map.get(document, Atom.to_string(name)), nil)
      {:value, value, field} ->
        preprocess.(field, Conversions.to_ecto_pk(value, pk), nil)
      field ->
        preprocess.(field, document, nil)
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
    Connection.storage_down(opts)
  end

  ## Migration

  alias Ecto.Migration.Table
  alias Ecto.Migration.Index

  @doc false
  def supports_ddl_transaction?, do: false

  @doc false
  def execute_ddl(_repo, string, _opts) when is_binary(string) do
    raise ArgumentError, "MongoDB adapter does not support SQL statements in `execute`"
  end

  def execute_ddl(repo, command, opts) when is_list(command) do
    command(repo, command, opts)
    :ok
  end

  def execute_ddl(repo, {:create, %Table{options: nil, name: coll}, columns}, opts) do
    warn_on_references!(columns)
    command(repo, [create: coll], opts)
    :ok
  end

  def execute_ddl(repo, {:create, %Table{options: options, name: coll}, columns}, opts)
      when is_list(options) do
    warn_on_references!(columns)
    command(repo, [create: coll] ++ options, opts)
    :ok
  end

  def execute_ddl(_repo, {:create, %Table{options: string}, _columns}, _opts)
      when is_binary(string) do
    raise ArgumentError, "MongoDB adapter does not support SQL statements as collection options"
  end

  def execute_ddl(repo, {:create, %Index{} = command}, opts) do
    index = [name: to_string(command.name),
             unique: command.unique,
             background: command.concurrently,
             key: Enum.map(command.columns, &{&1, 1}),
             ns: namespace(repo, command.table)]

    query = %WriteQuery{coll: "system.indexes", command: index}

    {:ok, _} = Connection.insert(repo, query, opts)
    :ok
  end

  def execute_ddl(repo, {:drop, %Index{name: name, table: coll}}, opts) do
    command(repo, [dropIndexes: coll, index: to_string(name)], opts)
    :ok
  end

  def execute_ddl(repo, {:drop, %Table{name: coll}}, opts) do
    command(repo, [drop: coll], opts)
    :ok
  end

  def execute_ddl(repo, {:rename, %Table{name: old}, %Table{name: new}}, opts) do
    command = [renameCollection: namespace(repo, old), to: namespace(repo, new)]
    command(repo, command, [database: "admin"] ++ opts)
    :ok
  end

  def execute_ddl(repo, {:rename, %Table{name: coll}, old, new}, opts) do
    query = %WriteQuery{coll: to_string(coll),
                        command: ["$rename": [{to_string(old), to_string(new)}]],
                        opts: [multi: true]}

    {:ok, _} = Connection.update(repo, query, opts)
    :ok
  end

  def execute_ddl(_repo, {:create_if_not_exists, %Table{options: nil}, columns}, _opts) do
    # We treat this as a noop as the collection will be created by mongo
    warn_on_references!(columns)
    :ok
  end

  def execute_ddl(_repo, {:create_if_not_exists, %Table{}, _columns}, _opts) do
    raise ArgumentError, "MongoDB adapter supports options for collection only in the `create` function"
  end

  def execute_ddl(_repo, {:create_if_not_exists, %Index{}}, _opts) do
    raise ArgumentError, "MongoDB adapter does not support `create_if_not_exists` for indexes"
  end

  def execute_ddl(_repo, {:drop_if_exists, _}, _opts) do
    raise ArgumentError, "MongoDB adapter does not support `drop_if_exists`"
  end

  defp warn_on_references!(columns) do
    has_references? =
      Enum.any?(columns, fn
        {_, _, %Ecto.Migration.Reference{}, _} -> true
        _other                                 -> false
      end)

    if has_references? do
      IO.puts "[warning] MongoDB adapter does not support references, and will not enforce foreign_key constraints"
    end
  end

  ## Mongo specific calls

  @doc """
  Drops all the collections in current database.

  Skips system collections and `schema_migrations` collection.
  Especially usefull in testing.

  Returns list of dropped collections.
  """
  @spec truncate(Ecto.Repo.t, Keyword.t) :: [String.t]
  def truncate(repo, opts \\ []) do
    opts = Keyword.put(opts, :log, false)

    Enum.map(list_collections(repo, opts), fn collection ->
      truncate_collection(repo, collection, opts)
      collection
    end)
  end

  @doc """
  Runs a command in the database.

  ## Usage

      Mongo.Ecto.command(Repo, drop: "collection")

  ## Options

    * `:database` - run command against a specific database
      (default: repo's database)
    * `:log` - should command queries be logged (default: true)

  For list of available commands plese see: http://docs.mongodb.org/manual/reference/command/
  """
  @spec command(Ecto.Repo.t, BSON.document, Keyword.t) :: BSON.document
  def command(repo, command, opts \\ []) do
    normalized = NormalizedQuery.command(command, opts)

    Connection.command(repo, normalized, opts)
  end

  special_regex = %BSON.Regex{pattern: "\\.system|\\$", options: ""}
  @migration Ecto.Migration.SchemaMigration.__schema__(:source)
  migration_regex = %BSON.Regex{pattern: @migration, options: ""}

  @list_collections_query ["$and": [[name: ["$not": special_regex]],
                                    [name: ["$not": migration_regex]]]]

  @doc false
  def list_collections(repo, opts \\ []) do
    list_collections(db_version(repo), repo, opts)
  end

  defp list_collections(version, repo, opts) when version >= 3 do
    colls = command(repo, %{"listCollections": 1}, opts)["cursor"]["firstBatch"]

    all_collections =
      colls
      |> Enum.map(&Map.fetch!(&1, "name"))
      |> Enum.reject(&String.contains?(&1, "system."))

    all_collections -- [@migration]
  end

  defp list_collections(_,repo, opts) do
    query = %ReadQuery{coll: "system.namespaces", query: @list_collections_query}
    opts = Keyword.put(opts, :log, false)

    Connection.read(repo, query, opts)
    |> Enum.map(&Map.fetch!(&1, "name"))
    |> Enum.map(fn collection ->
      collection |> String.split(".", parts: 2) |> Enum.at(1)
    end)
  end

  defp truncate_collection(repo, collection, opts) do
    query = %WriteQuery{coll: collection, query: %{}}
    Connection.delete_all(repo, query, opts)
  end

  defp namespace(repo, coll) do
    "#{repo.config[:database]}.#{coll}"
  end

  defp db_version(repo) do
    version = command(repo, %{"buildinfo": 1}, [])["versionArray"]

    Enum.fetch!(version, 0)
  end
end
