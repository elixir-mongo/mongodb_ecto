defmodule Mongo.Ecto do
  @moduledoc """
  Ecto integration with MongoDB.

  This document will present a general overview of using MongoDB with Ecto,
  including common pitfalls and extra functionalities.

  Check the [Ecto documentation](http://hexdocs.pm/ecto) for an introduction
  or [examples/simple](https://github.com/ankhers/mongodb_ecto/tree/master/examples/simple)
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
  `count` command. Please note that unlike in SQL databases you can only select
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
  We support them through the `Mongo.Ecto.command/2` function.

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
  there is one field where they can be very beneficial - indexes. Because of
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
    * `:mongo_url` - A MongoDB [URL](https://docs.mongodb.com/manual/reference/connection-string/)
    * `:connect_timeout` - The timeout for establishing new connections
       (default: 5000)
    * `:w` - MongoDB's write convern (default: 1). If set to 0, some of the
      Ecto's functions may not work properely
    * `:j`, `:fsync`, `:wtimeout` - Other MongoDB's write concern options.
      Please consult MongoDB's documentation

  ### Pool options

  `Mongo.Ecto` does not use Ecto pools, instead pools provided by the MongoDB
  driver are used. The default poolboy adapter accepts following options:

    * `:pool_size` - The number of connections to keep in the pool (default: 10)
    * `:max_overflow` - The maximum overflow of connections (default: 0)

  For other adapters, please see their documentation.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  alias Mongo.Ecto.Connection
  alias Mongo.Ecto.Conversions
  alias Mongo.Ecto.NormalizedQuery

  alias Mongo.Ecto.NormalizedQuery.{
    AggregateQuery,
    CountQuery,
    ReadQuery,
    WriteQuery
  }

  ## Adapter

  @doc false
  @impl true
  defmacro __before_compile__(_env) do
  end

  @pool_timeout 5_000
  @timeout 15_000

  defp normalize_config(config) do
    config
    |> Keyword.delete(:name)
    |> Keyword.put_new(:timeout, @timeout)
    |> Keyword.put_new(:pool_timeout, @pool_timeout)
  end

  defp pool_name(config) do
    Keyword.get(config, :pool_name, default_pool_name(config))
  end

  defp default_pool_name(config) do
    Module.concat(Keyword.get(config, :name, config[:repo]), Pool)
  end

  @doc false
  def application, do: :mongodb_ecto

  @pool_opts [:timeout, :pool, :pool_size, :migration_lock] ++
               [:queue_target, :queue_interval, :ownership_timeout]

  @impl true
  def init(config) do
    connection = Connection

    unless Code.ensure_loaded?(connection) do
      driver = :mongodb

      raise """
      Could not find #{inspect(connection)}.

      Please verify you have added #{inspect(driver)} as a dependency to mix.exs:
          {#{inspect(driver)}, ">= 0.0.0"}

      Remember to recompile Ecto afterwards by cleaning the current build:
          mix deps.clean --build ecto
      """
    end

    pool_name = pool_name(config)
    norm_config = normalize_config(config)

    log = Keyword.get(config, :log, :debug)
    telemetry_prefix = Keyword.fetch!(config, :telemetry_prefix)
    telemetry = {config[:repo], log, telemetry_prefix ++ [:query]}

    opts = Keyword.take(config, @pool_opts)
    meta = %{telemetry: telemetry, opts: opts, pool: {pool_name, norm_config}}
    {:ok, connection.child_spec(config), meta}
  end

  @impl true
  def ensure_all_started(_repo, type) do
    {:ok, _mongo} = Application.ensure_all_started(:mongodb, type)
  end

  @impl true
  def loaders(:time, type), do: [&load_time/1, type]
  def loaders(:date, type), do: [&load_date/1, type]
  def loaders(:utc_datetime, type), do: [&load_datetime/1, type]
  def loaders(:utc_datetime_usec, type), do: [&load_datetime/1, type]
  def loaders(:naive_datetime, type), do: [&load_datetime/1, type]
  def loaders(:naive_datetime_usec, type), do: [&load_datetime/1, type]
  def loaders(:binary_id, type), do: [&load_objectid/1, type]
  def loaders(:uuid, type), do: [&load_binary/1, type]
  def loaders(:binary, type), do: [&load_binary/1, type]
  def loaders(:integer, type), do: [&load_integer/1, type]

  def loaders(_base, type) do
    [type]
  end

  defp load_time(time), do: time

  defp load_date(date) do
    {:ok, date |> DateTime.to_date()}
  end

  defp load_datetime(datetime) do
    {:ok, datetime}
  end

  defp load_integer(map) do
    {:ok, map}
  end

  defp load_binary(%BSON.Binary{binary: binary}), do: {:ok, binary}
  defp load_binary(_), do: :error

  defp load_objectid(%BSON.ObjectId{} = objectid) do
    {:ok, BSON.ObjectId.encode!(objectid)}
  rescue
    ArgumentError ->
      :error
  end

  defp load_objectid(_arg), do: :error

  @impl true
  def dumpers(:time, type), do: [type, &dump_time/1]
  def dumpers(:date, type), do: [type, &dump_date/1]
  def dumpers(:utc_datetime, type), do: [type, &dump_utc_datetime/1]
  def dumpers(:utc_datetime_usec, type), do: [type, &dump_utc_datetime/1]
  def dumpers(:naive_datetime, type), do: [type, &dump_naive_datetime/1]
  def dumpers(:naive_datetime_usec, type), do: [type, &dump_naive_datetime/1]
  def dumpers(:binary_id, type), do: [type, &dump_objectid/1]
  def dumpers(:uuid, type), do: [type, &dump_binary(&1, :uuid)]
  def dumpers(:binary, type), do: [type, &dump_binary(&1, :generic)]
  def dumpers(_base, type), do: [type]

  defp dump_time({h, m, s, _}), do: Time.from_erl({h, m, s})
  defp dump_time(%Time{} = time), do: time
  defp dump_time(_), do: :error

  defp dump_date({_, _, _} = date) do
    dt =
      {date, {0, 0, 0}}
      |> NaiveDateTime.from_erl!()
      |> DateTime.from_naive!("Etc/UTC")

    {:ok, dt}
  end

  defp dump_date(%Date{} = date) do
    {:ok, date}
  end

  defp dump_date(_) do
    :error
  end

  defp dump_utc_datetime({{_, _, _} = date, {h, m, s, ms}}) do
    datetime =
      {date, {h, m, s}}
      |> NaiveDateTime.from_erl!({ms, 6})
      |> DateTime.from_naive!("Etc/UTC")

    {:ok, datetime}
  end

  defp dump_utc_datetime({{_, _, _} = date, {h, m, s}}) do
    datetime =
      {date, {h, m, s}}
      |> NaiveDateTime.from_erl!({0, 6})
      |> DateTime.from_naive!("Etc/UTC")

    {:ok, datetime}
  end

  defp dump_utc_datetime(datetime) do
    {:ok, datetime}
  end

  defp dump_naive_datetime({{_, _, _} = date, {h, m, s, ms}}) do
    datetime =
      {date, {h, m, s}}
      |> NaiveDateTime.from_erl!({ms, 6})
      |> DateTime.from_naive!("Etc/UTC")

    {:ok, datetime}
  end

  defp dump_naive_datetime(%NaiveDateTime{} = dt) do
    datetime =
      dt
      |> DateTime.from_naive!("Etc/UTC")

    {:ok, datetime}
  end

  defp dump_naive_datetime(dt) do
    datetime =
      dt
      |> DateTime.from_naive!("Etc/UTC")

    {:ok, datetime}
  end

  defp dump_binary(binary, subtype) when is_binary(binary),
    do: {:ok, %BSON.Binary{binary: binary, subtype: subtype}}

  defp dump_binary(_, _), do: :error

  defp dump_objectid(<<objectid::binary-size(24)>>) do
    {:ok, BSON.ObjectId.decode!(objectid)}
  rescue
    ArgumentError -> :error
  end

  defp dump_objectid(_), do: :error

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: raise("MongoDB adapter does not support `:id` type as primary key")
  def autogenerate(:embed_id), do: BSON.ObjectId.encode!(Mongo.object_id())
  # :binary_id is expected to be generated by Mongo itself as a BSON.ObjectId
  def autogenerate(:binary_id), do: nil

  @impl Ecto.Adapter.Queryable
  def prepare(function, query) do
    {:nocache, {function, query}}
  end

  @read_queries [ReadQuery, CountQuery, AggregateQuery]

  @impl Ecto.Adapter.Queryable
  def execute(meta, _query_meta, {:nocache, {function, query}}, params, opts) do
    struct = get_struct_from_query(query)

    case apply(NormalizedQuery, function, [query, params]) do
      %AggregateQuery{} = query ->
        {rows, count} =
          Connection.read(meta, query, opts)
          |> Enum.map_reduce(0, &{[&1["value"]], &2 + 1})

        {count, rows}

      %CountQuery{} = query ->
        {rows, count} =
          Connection.read(meta, query, opts)
          |> Enum.map_reduce(0, &{[&1["value"]], &2 + 1})

        {count, rows}

      %ReadQuery{} = query ->
        {rows, count} =
          Connection.read(meta, query, opts)
          |> Enum.map_reduce(0, &{process_document(&1, query, struct), &2 + 1})

        {count, rows}

      %WriteQuery{} = write ->
        result = apply(Connection, function, [meta, write, opts])
        {result, nil}
    end
  end

  def row_to_list(row, %{select: %{from: {_op, {_source, _tuple, _something, types}}}}) do
    Enum.map(types, fn {field, _type} ->
      case field do
        :id -> row["_id"]
        _ -> row[Atom.to_string(field)]
      end
    end)
  end

  def row_to_list(row, %{select: %{from: :none}}) do
    [row]
  end

  # This can be backed by a normal mongo stream, we just have to get it to play nicely with
  #  ecto's batch/preload functionality ( hence the map(&{nil, [&1]}) )
  @impl Ecto.Adapter.Queryable
  def stream(adapter_meta, _query_meta, {:nocache, {function, query}}, params, opts) do
    struct = get_struct_from_query(query)

    case apply(NormalizedQuery, function, [query, params]) do
      %{__struct__: read} = query when read in @read_queries ->
        Connection.read(adapter_meta, query, opts)
        |> Stream.map(&process_document(&1, query, struct))

      %WriteQuery{} = write ->
        apply(Connection, function, [adapter_meta, write, opts])
        [nil]
    end
    |> Stream.map(&{nil, [&1]})
  end

  defp get_struct_from_query(%Ecto.Query{from: %Ecto.Query.FromExpr{source: {_coll, nil}}}),
    do: nil

  defp get_struct_from_query(%Ecto.Query{from: %Ecto.Query.FromExpr{source: {_coll, struct}}}),
    do: struct.__struct__()

  defp get_struct_from_query(_), do: nil

  @impl Ecto.Adapter.Schema
  def insert(adapter_meta, schema_meta, fields, on_conflict, returning, opts) do
    normalized_query = NormalizedQuery.insert(schema_meta, fields, on_conflict, returning, opts)

    apply(Connection, normalized_query.op, [adapter_meta, normalized_query, opts])
  end

  defp normalise_id(doc) do
    doc
    |> Enum.map(fn
      {:id, id} -> {:_id, id}
      other -> other
    end)
  end

  @impl Ecto.Adapter.Schema
  # def insert_all(adapter_meta, schema_meta, header, fields_list, on_conflict, returning, placeholders, opts) do
  #   IO.inspect(header, label: "header")
  #   IO.inspect(fields_list, label: "fields")
  #   IO.inspect(on_conflict, label: "on_conflict")
  #   IO.inspect(returning, label: "returning")
  #   IO.inspect(placeholders, label: "placeholders")

  #   normalized_query = NormalizedQuery.insert(schema_meta, fields_list, on_conflict, returning, opts)

  #   IO.inspect(normalized_query, label: "normalized query")

  #   case apply(Connection, normalized_query.op, [adapter_meta, normalized_query, opts]) |> IO.inspect(label: "response") do
  #     {:ok, _} ->
  #       {:ok, []}

  #     other ->
  #       other
  #   end
  # end
  def insert_all(
        repo,
        meta,
        _fields,
        params,
        {[_ | _] = replace_fields, _, conflict_targets},
        _returning,
        _placeholders,
        opts
      ) do
    # need to do a check that the docs contain the specified conflict fields
    # docs
    command =
      params
      |> Enum.map(fn doc ->
        [
          query: conflict_targets |> Enum.map(fn target -> {target, doc[target]} end),
          update: [
            "$set":
              replace_fields |> Enum.map(fn field -> {field, doc[field]} end) |> normalise_id(),
            "$setOnInsert":
              doc
              |> Enum.filter(fn {k, _v} -> k not in replace_fields end)
              |> normalise_id()
          ],
          upsert: true
        ]
      end)

    normalized = %Mongo.Ecto.NormalizedQuery.WriteQuery{
      coll: meta.source,
      command: command,
      database: nil,
      opts: [],
      query: %{}
    }

    case Connection.update(repo, normalized, opts) do
      {:ok, n_modified} -> {n_modified, nil}
      other -> other
    end
  end

  def insert_all(
        repo,
        meta,
        _fields,
        params,
        {:nothing, _list, conflict_targets},
        _returning,
        _placeholders,
        opts
      ) do
    if :id in conflict_targets do
      raise "Primary key not allowed as a target?!"
    end

    # docs
    command =
      params
      |> Enum.map(fn doc ->
        query =
          case conflict_targets do
            [] ->
              %{}

            conflict_targets ->
              conflict_targets |> Enum.map(fn target -> {target, doc[target]} end)
          end

        [
          query: query,
          update: [
            "$setOnInsert": doc
          ],
          upsert: true
        ]
      end)

    normalized = %Mongo.Ecto.NormalizedQuery.WriteQuery{
      coll: meta.source,
      command: command,
      database: nil,
      opts: [],
      query: %{}
    }

    case Connection.update(repo, normalized, opts) do
      {:ok, n_modified} -> {n_modified, nil}
      other -> other
    end
  end

  def insert_all(
        repo,
        meta,
        fields,
        params,
        {%Ecto.Query{} = query, values, conflict_targets},
        returning,
        placeholders,
        opts
      ) do
    # values |> IO.inspect(label: "values")
    # fields |> IO.inspect(label: "fields")
    # params |> IO.inspect(label: "params")
    # conflict_targets |> IO.inspect(label: "conflict_targets")
    # returning |> IO.inspect(label: "returning")
    # placeholders |> IO.inspect(label: "placeholders")
    # opts |> IO.inspect(label: "opts")

    if :id in conflict_targets do
      raise "Primary key not allowed as a target?!"
    end

    conflict_targets = opts |> Keyword.get(:conflict_target)

    # docs
    command =
      params
      |> Enum.map(fn doc ->
        %{query: find_query} = query |> NormalizedQuery.all(values)

        %{command: update} = query |> NormalizedQuery.update_all(Keyword.values(doc) ++ values)

        set_fields = update[:"$set"]

        doc =
          case Map.get(update, :"$inc") do
            nil ->
              doc

            increment_fields ->
              # { $add: [ "$price", "$fee" ]
              increment_fields =
                increment_fields
                |> Enum.map(fn {k, v} ->
                  {k, %{"$add": ["$#{k}", v]}}
                end)

              doc ++ increment_fields
          end

        [
          query:
            case conflict_targets do
              conflict_targets when is_list(conflict_targets) ->
                conflict_targets |> Enum.map(fn target -> {target, doc[target]} end)

              conflict_target when is_atom(conflict_target) ->
                Keyword.put([], conflict_target, doc[conflict_target])
            end,
          update: [
            %{
              "$project":
                doc
                |> Enum.reduce(%{}, fn
                  {:id, v}, acc ->
                    Map.put(acc, :_id, %{"$cond": ["$_id", "$REMOVE", v]})

                  {k, v}, acc ->
                    if set_fields && Keyword.has_key?(set_fields, k) do
                      Map.put(acc, k, %{"$cond": ["$_id", Keyword.get(set_fields, k), v]})
                    else
                      Map.put(acc, k, v)
                    end
                end)
            }
          ],
          upsert: true
        ]
      end)

    normalized = %Mongo.Ecto.NormalizedQuery.WriteQuery{
      coll: meta.source,
      command: command,
      database: nil,
      opts: [],
      query: %{}
    }

    case Connection.update(repo, normalized, opts) do
      {:ok, n_modified} -> {n_modified, nil}
      other -> other
    end
  end

  def insert_all(repo, meta, fields, params, on_conflict, returning, placeholders, opts) do
    normalized = NormalizedQuery.insert(meta, params)

    case Connection.insert_all(repo, normalized, opts) |> IO.inspect(label: "insert_all res") do
      {:ok, _} ->
        {:ok, []}

      {:invalid, constraints} ->
        if {:raise, _, _} = on_conflict do
          raise "should be original error raised here"
        else
          {:invalid, constraints}
        end

      other ->
        other
    end
  end

  @impl Ecto.Adapter.Schema
  def update(repo, meta, fields, filters, _returning, opts) do
    {repo, meta, fields, filters, opts}

    normalized = NormalizedQuery.update_one(meta, fields, filters)

    Connection.update_one(repo, normalized, opts)
  end

  @impl true
  def delete(repo, meta, filter, opts) do
    normalized = NormalizedQuery.delete(meta, filter)

    Connection.delete(repo, normalized, opts)
  end

  defp process_document(document, %{fields: fields, pk: pk}, struct) do
    document = Conversions.to_ecto_pk(document, pk || :_id)

    Enum.map(fields, fn
      {:field, name, _field} ->
        # If we don't have the key but do a have a struct, we get the default.
        # Otherwise, we get get the value from the doc
        if Map.has_key?(document, Atom.to_string(name)) == false && struct != nil do
          Map.get(struct, name)
        else
          Map.get(document, Atom.to_string(name))
        end

      {:value, value, _field} ->
        Conversions.to_ecto_pk(value, pk)

      _field ->
        document
    end)
  end

  # TODO Not sure how to do this or if it's useful for Mongo
  @impl true
  def checkout(_, _, fun) do
    fun.()
  end

  ## Storage

  # Noop for MongoDB, as any databases and collections are created as needed.
  @impl true
  def storage_up(_opts) do
    :ok
  end

  @impl true
  def storage_down(opts) do
    Connection.storage_down(opts)
  end

  @impl true
  def storage_status(opts) do
    Connection.storage_status(opts)
  end

  ## Mongo specific calls

  special_regex = %BSON.Regex{pattern: "\\.system|\\$", options: ""}
  # migration_regex = %BSON.Regex{pattern: @migration, options: ""}
  @list_collections_query [
    [name: ["$not": special_regex]]
  ]

  @doc """
  Drops all the collections in current database.

  Skips system collections and `schema_migrations` collection.
  Especially useful in testing.

  Returns list of dropped collections.
  """
  @spec truncate(Ecto.Repo.t(), Keyword.t()) :: [String.t()]
  def truncate(repo, opts \\ []) do
    opts = Keyword.put(opts, :log, false)
    version = db_version(repo)
    [major_version, minor_version | _] = version

    collection_names =
      if major_version > 3 || (major_version == 3 && minor_version >= 4) do
        _all_collection_names =
          repo
          |> command(%{listCollections: 1}, opts)
          |> get_in(["cursor", "firstBatch"])
          # exclude mongo views which were introduced in version 3.4
          |> Enum.filter(&(&1["type"] == "collection"))
          |> Enum.map(&Map.fetch!(&1, "name"))
          |> Enum.reject(&String.contains?(&1, "system."))

        # all_collection_names -- [@migration]
      else
        list_collections(version, repo, opts)
      end

    Enum.map(collection_names, fn collection ->
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
  @spec command(Ecto.Repo.t(), BSON.document(), Keyword.t()) :: BSON.document()
  def command(repo, command, opts \\ []) do
    normalized = NormalizedQuery.command(command, opts)

    Connection.command(Ecto.Adapter.lookup_meta(repo), normalized, opts)
  end

  @doc false
  def list_collections(repo, opts \\ []) do
    list_collections(db_version(repo), repo, opts)
  end

  defp list_collections([major_version | _], repo, opts) when major_version >= 3 do
    colls = command(repo, %{listCollections: 1}, opts)["cursor"]["firstBatch"]

    _all_collections =
      colls
      |> Enum.map(&Map.fetch!(&1, "name"))
      |> Enum.reject(&String.contains?(&1, "system."))

    # all_collections -- [@migration]
  end

  defp list_collections(_, repo, opts) do
    query = %ReadQuery{coll: "system.namespaces", query: @list_collections_query}
    opts = Keyword.put(opts, :log, false)

    Connection.read(repo, query, opts)
    |> Enum.map(&Map.fetch!(&1, "name"))
    |> Enum.map(fn collection ->
      collection |> String.split(".", parts: 2) |> Enum.at(1)
    end)
  end

  defp truncate_collection(repo, collection, opts) do
    meta = Ecto.Adapter.lookup_meta(repo)
    query = %WriteQuery{coll: collection, query: %{}}
    Connection.delete_all(meta, query, opts)
  end

  defp db_version(repo) do
    command(repo, %{buildinfo: 1}, [])["versionArray"]
  end

  @doc """
  Lists indexes in the specified `repo` and `collection`.
  """
  def list_indexes(repo, collection, opts \\ []) do
    Ecto.Adapter.lookup_meta(repo)
    |> Connection.query(:list_indexes, [collection], opts)
    |> Enum.to_list()
  end

  def list_index_names(repo, collection, opts \\ []) do
    Ecto.Adapter.lookup_meta(repo)
    |> Connection.query(:list_index_names, [collection], opts)
    |> Enum.to_list()
  end

  def index(repo, collection, index_name, opts \\ []) do
    list_indexes(repo, collection, opts)
    |> Enum.find(fn index -> index["name"] == index_name end)
  end

  @doc """
  Creates one or more `indexes` for the specified collection `coll`.

  See
  https://docs.mongodb.com/manual/reference/method/db.collection.createIndexes/#mongodb-method-db.collection.createIndexes
  for the syntax of `indexes`.
  """
  def create_indexes(repo, collection, indexes, opts \\ []) do
    Ecto.Adapter.lookup_meta(repo)
    |> Connection.query(:create_indexes, [collection, indexes], opts)
  end

  @doc """
  Drops the specified `indexes` in the collection `coll`.

  To drop a single index, pass the name of the index.

  To drop multiple indexes at once pass a list of indexes to `index`.  To drop all indexes except
  that of `_id` pass "*" to `index`.

  See https://docs.mongodb.com/manual/reference/command/dropIndexes/#dropindexes
  """
  def drop_indexes(repo, collection, indexes, opts \\ []) do
    Ecto.Adapter.lookup_meta(repo)
    |> Connection.query(:drop_index, [collection, indexes], opts)
  end
end
