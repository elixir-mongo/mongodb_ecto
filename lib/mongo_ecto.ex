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

  There is also support for count function in queries that uses `MongoDB`
  `count` command. Please not that unline in SQL databases you can only select
  a count - there is no support for querying using a count, there is also no
  support for counting documents and selecting them at the same time.

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

  Ecto supports database migrations. You can generate a migration with:

      $ mix ecto.gen.migration create_posts

  This will create a new file inside `priv/repo/migrations` with the `up` and
  `down` functions. Check `Ecto.Migration` for more information.

  Keep in mind that MongoDB does not support (or need) database schemas, so
  majority of the functionality provided by `Ecto.Migration` is not useful when
  working with MongoDB. The usefull elements include creating indexes, capped
  collections, executing commands or migrating data, e.g.:

      defmodule SampleMigration do
        use Ecto.Migration

        def up do
          create table(:my_table, options: [capped: true, size: 1024])
          create index(:my_table, [:value], unique: true)
          execute touch: "my_table", data: true, index: true
        end

        def down do
          # ...
        end
      end

  MongoDB adapter does not support `create_if_not_exists` and `drop_if_exists`
  migration functions.

  ## MongoDB adapter features

  The adapter uses `mongodb` for communicating with the database and a pooling
  library such as `poolboy` for managing connections.

  The adapter has:

    * Support for documents with ObjectID as their primary key
    * Support for insert, find, update and remove and count mongo functions
    * Support for management commands with `command/2`
    * Support for embedded objects with `:map` and `{:array, :map}` types

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
    * `:j`, `:fsync`, `:wtimeout` - Other MongoDV's write concern options. Pleas
      consult MongoDB's documentation

  ### Pool options

  `Mongo.Ecto` does not use Ecto pools, instead pools provided by the MongoDB
  driver are used. The default poolboy adapter accepts following options:

    * `:size` - The number of connections to keep in the pool (default: 10)
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
  alias Mongo.Ecto.Decoder
  alias Mongo.Ecto.ObjectID
  alias Mongo.Ecto.Connection

  ## Adapter

  @doc false
  defmacro __before_compile__(env) do
    module = env.module
    config = Module.get_attribute(module, :config)
    adapter = Keyword.get(config, :pool, Mongo.Pool.Poolboy)

    quote do
      defmodule Pool do
        use Mongo.Pool, name: __MODULE__, adapter: unquote(adapter)
      end

      def __mongo_pool__, do: unquote(module).Pool
    end
  end

  @doc false
  def start_link(repo, opts) do
    {:ok, _} = Application.ensure_all_started(:mongodb_ecto)

    repo.__mongo_pool__.start_link(opts)
  end

  @doc false
  def load(:binary_id, data),
    do: Ecto.Type.load(ObjectID, data, &load/2)
  def load(Ecto.Date, {date, _time}),
    do: Ecto.Type.load(Ecto.Date, date, &dump/2)
  def load(type, data),
    do: Ecto.Type.load(type, data, &load/2)

  @doc false
  def dump(:binary_id, data),
    do: Ecto.Type.dump(ObjectID, data, &dump/2)
  def dump(Ecto.Date, %Ecto.Date{} = data),
    do: Ecto.Type.dump(Ecto.DateTime, Ecto.DateTime.from_date(data), &dump/2)
  def dump(type, data),
    do: Ecto.Type.dump(type, data, &dump/2)

  @doc false
  def embed_id(_), do: ObjectID.generate

  @doc false
  def all(repo, query, params, preprocess, opts) do
    case NormalizedQuery.all(query, params) do
      %ReadQuery{} = read ->
        Connection.all(repo.__mongo_pool__, read, opts)
        |> Enum.map(&process_document(&1, read, preprocess))
      %CountQuery{} = command ->
        [[Connection.count(repo.__mongo_pool__, command, opts)]]
    end
  end

  @doc false
  def update_all(repo, query, params, opts) do
    normalized = NormalizedQuery.update_all(query, params)

    {Connection.update_all(repo.__mongo_pool__, normalized, opts), nil}
  end

  @doc false
  def delete_all(repo, query, params, opts) do
    normalized = NormalizedQuery.delete_all(query, params)

    {Connection.delete_all(repo.__mongo_pool__, normalized, opts), nil}
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

    {:ok, _} = Connection.insert(repo.__mongo_pool__, normalized, opts)
    {:ok, []}
  end

  def insert(repo, source, params, {pk, :binary_id, nil}, [], opts) do
    normalized = NormalizedQuery.insert(source, params, pk)

    {:ok, %{inserted_id: %BSON.ObjectId{value: value}}} =
      Connection.insert(repo.__mongo_pool__, normalized, opts)
    {:ok, [{pk, value}]}
  end

  def insert(repo, source, params, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.insert(source, params, pk)

    {:ok, _} = Connection.insert(repo.__mongo_pool__, normalized, opts)
    {:ok, []}
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

    Connection.update(repo.__mongo_pool__, normalized, opts)
  end

  @doc false
  def delete(_repo, source, _filter, {key, :id, _}, _opts) do
    raise ArgumentError, "MongoDB adapter does not support :id field type in models. " <>
                         "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def delete(repo, source, filter, {pk, :binary_id, _value}, opts) do
    normalized = NormalizedQuery.delete(source, filter, pk)

    Connection.delete(repo.__mongo_pool__, normalized, opts)
  end

  defp process_document(document, %{fields: fields, pk: pk}, preprocess) do
    document = Decoder.decode_document(document, pk)

    Enum.map(fields, fn
      {:field, name, field} ->
        preprocess.(field, Map.get(document, Atom.to_string(name)))
      {:value, value, field} ->
        preprocess.(field, Decoder.decode_value(value, pk))
      field ->
        preprocess.(field, document)
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

  def execute_ddl(repo, {:create, %Table{options: nil, name: coll}, _columns}, opts) do
    command(repo, [create: coll], opts)
    :ok
  end

  def execute_ddl(repo, {:create, %Table{options: options, name: coll}, _columns}, opts)
      when is_list(options) do
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

    Connection.insert(repo.__mongo_pool__, query, opts)
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

    Connection.update(repo.__mongo_pool__, query, opts)
    :ok
  end

  def execute_ddl(_repo, {:create_if_not_exists, %Table{options: nil}, _columns}, _opts) do
    # We treat this as a noop as the collection will be created by mongo
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

  ## Mongo specific calls

  @doc """
  Drops all the collections in current database.

  Skips system collections and `schema_migrations` collection.

  Especially usefull in testing.
  """
  def truncate(repo, opts \\ []) do
    opts = Keyword.put(opts, :log, false)

    Enum.map(list_collections(repo, opts), fn collection ->
      drop_collection(repo, collection, opts)
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
  def command(repo, command, opts \\ []) do
    normalized = NormalizedQuery.command(command, opts)

    Connection.command(repo.__mongo_pool__, normalized, opts)
  end

  special_regex = %BSON.Regex{pattern: "\\.system|\\$", options: ""}
  migration = Ecto.Migration.SchemaMigration.__schema__(:source)
  migration_regex = %BSON.Regex{pattern: migration, options: ""}

  @list_collections_query ["$and": [[name: ["$not": special_regex]],
                                    [name: ["$not": migration_regex]]]]

  defp list_collections(repo, opts) do
    query = %ReadQuery{coll: "system.namespaces", query: @list_collections_query}
    opts = Keyword.put(opts, :log, false)

    Connection.all(repo.__mongo_pool__, query, opts)
    |> Enum.map(&Map.fetch!(&1, "name"))
    |> Enum.map(fn collection ->
      collection |> String.split(".", parts: 2) |> Enum.at(1)
    end)
  end

  defp drop_collection(repo, collection, opts) do
    command(repo, [drop: collection], opts)
  end

  defp namespace(repo, coll) do
    "#{repo.config[:database]}.#{coll}"
  end
end
