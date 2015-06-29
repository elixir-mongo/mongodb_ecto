defmodule Mongo.Ecto do
  @moduledoc """
  Ecto is split into 3 main components:

    * `Ecto.Repo` - repositories are wrappers around the database.
      Via the repository, we can create, update, destroy and query existing entries.
      A repository needs an adapter and a URL to communicate to the database

    * `Ecto.Model` - models provide a set of functionalities for defining
      data structures, how changes are performed in the storage, life-cycle
      callbacks and more

    * `Ecto.Query` - written in Elixir syntax, queries are used to retrieve
      information from a given repository. Queries in Ecto provide type safety.
      Queries are composable via the `Ecto.Queryable` protocol

  In the following sections, we will provide an overview of those components and
  how they interact with each other as well as explain specifics of using
  MongoDB with Ecto as your datastore. Feel free to access their respective module
  documentation for more specific examples, options and configuration.

  If you want to quickly check a sample application using Ecto and MongoDB,
  please check https://github.com/michalmuskala/mongodb_ecto/tree/master/examples/simple.

  ## Ecto repositories

  `Ecto.Repo` is a wrapper around the database. We can define a
  repository as follows:

      defmodule Repo do
        use Ecto.Repo, otp_app: :my_app
      end

  Where the configuration for the Repo must be in your application
  environment, usually defined in your `config/config.exs`:

      config :my_app, Repo,
        adapter: Mongo.Ecto,
        database: "ecto_simple",
        username: "mongodb",
        password: "mongodb",
        hostname: "localhost"

  Each repository in Ecto defines a `start_link/0` function that needs to be invoked
  before using the repository. In general, this function is not called directly,
  but used as part of your application supervision tree.

  If your application was generated with a supervisor (by passing `--sup` to `mix new`)
  you will have a `lib/my_app.ex` file containing the application start callback that
  defines and starts your supervisor. You just need to edit the `start/2` function to
  start the repo as a worker on the supervisor:

      def start(_type, _args) do
        import Supervisor.Spec

        children = [
          worker(Repo, [])
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  ## Ecto models

  Models provide a set of functionalities around structuring your data,
  defining relationships and applying changes to repositories.

  For now, we will cover two of those:

    * `Ecto.Schema` - provides the API necessary to define schemas
    * `Ecto.Changeset` - defines how models should be changed in the database

  Let's see an example:
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
  handle as ObjectID. Remember to place this declaration before the `schema` call.
  The name of the primary key is just a syntactic sugar, as MongoDB forces us to
  use `_id`. Every other name will be recursively changed to `_id` in all calls
  to the adapter. We propose to use `id` or `_id` as your primary key name
  to limit eventual confusion, but you are free to use whatever you like.
  Using the `autogenrate: true` option will tell the adapter to take care of
  generating new ObjectIDs. Otherwise you need to do this yourself.

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

  Notice how the storage (repository) and the data are decoupled. This provides
  two main benefits:

    * By having structs as data, we guarantee they are light-weight,
      serializable structures. In many languages, the data is often represented
      by large, complex objects, with entwined state transactions, which makes
      serialization, maintenance and understanding hard;

    * By making the storage explicit with repositories, we don't pollute the
      repository with unnecessary overhead, providing straight-forward and
      performant access to storage;

  ## Ecto changesets

  Although in the example above we have directly inserted and updated the
  model in the repository, most of the times, developers will use changesets
  to perform those operations.

  Changesets allow developers to filter, cast, and validate changes before
  we apply them to a model. Imagine the given model:

      defmodule User do
        use Ecto.Model

        schema "users" do
          field :name
          field :email
          field :age, :integer
        end

        def changeset(user, params \\ nil) do
          user
          |> cast(params, ~w(name email), ~w(age))
          |> validate_format(:email, ~r/@/)
          |> validate_inclusion(:age, 0..130)
          |> validate_unique(:email, on: Repo)
        end
      end

  Since `Ecto.Model` by default imports `Ecto.Changeset` functions,
  we use them to generate and manipulate a changeset in the `changeset/2`
  function above.

  First we invoke `Ecto.Changeset.cast/4` with the model, the parameters
  and a list of required and optional fields; this returns a changeset.
  The parameter is a map with binary keys and a value that will be cast
  based on the type defined on the model schema.

  Any parameter that was not explicitly listed in the required or
  optional fields list will be ignored. Furthermore, if a field is given
  as required but it is not in the parameter map nor in the model, it will
  be marked with an error and the changeset is deemed invalid.

  After casting, the changeset is given to many `Ecto.Changeset.validate_*/2`
  functions that validate only the **changed fields**. In other words:
  if a field was not given as a parameter, it won't be validated at all.
  For example, if the params map contain only the "name" and "email" keys,
  the "age" validation won't run.

  As an example, let's see how we could use the changeset above in
  a web application that needs to update users:

      def update(id, params) do
        changeset = User.changeset Repo.get!(User, id), params["user"]

        if changeset.valid? do
          user = Repo.update!(changeset)
          send_resp conn, 200, "Ok"
        else
          send_resp conn, 400, "Bad request"
        end
      end

  The `changeset/2` function receives the user model and its parameters
  and returns a changeset. If the changeset is valid, we persist the
  changes to the database, otherwise, we handle the error by emitting
  a bad request code.

  Another example to create users:

      def create(id, params) do
        changeset = User.changeset %User{}, params["user"]

        if changeset.valid? do
          user = Repo.insert!(changeset)
          send_resp conn, 200, "Ok"
        else
          send_resp conn, 400, "Bad request"
        end
      end

  The benefit of having explicit changesets is that we can easily provide
  different changesets for different use cases. For example, one
  could easily provide specific changesets for create and update:

      def changeset(user, :create, params) do
        # Changeset on create
      end

      def changeset(user, :update, params) do
        # Changeset on update
      end

  ## Queries in Ecto

  Last but not least, Ecto allows you to write queries in Elixir and send
  them to the repository, which translates them to the underlying database.
  Let's see an example:

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

  Examples and detailed documentation for each of those are available in the
  `Ecto.Query` module.

  When writing a query, you are inside Ecto's query syntax. In order to
  access params values or invoke functions, you need to use the `^`
  operator, which is overloaded by Ecto:

      def min_prcp(min) do
        from w in Weather, where: w.prcp > ^min or is_nil(w.prcp)
      end

  Besides `Repo.all/1`, which returns all entries, repositories also
  provide `Repo.one/1`, which returns one entry or nil, and `Repo.one!/1`
  which returns one entry or raises.

  Please note that not all Ecto queries are valid Mongo queries. The adapter
  will raise `Ecto.QueryError` if it encounters one, and will try to be as
  specific as possible as to what exactly is causing the problem.

  For things that are not possible to express with Elixir's syntax in queries,
  you can use keyword fragments:

      from p in Post, where: fragment("$exists": "name"), select: p

  To ease using more advanced queries there is `Mongo.Ecto.Helpers` module you
  should import into modules dealing with queries. Currently it defines two
  functions:

    * `javascript/2` to use inline JavaScript
    * `regex/2` to use regex objects


      from p in Post, where: ^javascript("this.visits === count", count: 10)
      from p in Post, where: fragment(title: ^regex("elixir", "i"))

  Please see the documentation of the `Mongo.Ecto.Helpers` module for more
  information and supported options.

  ### Options for `Repo.all/2`

  The `Repo.all/2` function accepts a keyword of options. MongoDB adapter allows
  you to use parameters specific to MongoDB `find` function:

  * `:slave_ok` - the read operation may run on secondary replica set member
  * `:partial` - partial data from a query against a sharded cluster in which
    some shards do not respond will be returned in stead of raising error

  ## Other topics related to Ecto

  ### Mix tasks and generators

  Ecto provides many tasks to help your workflow as well as code generators.
  You can find all available tasks by typing `mix help` inside a project
  with Ecto listed as a dependency.

  Ecto generators will automatically open the generated files if you have
  `ECTO_EDITOR` set in your environment variable.

  ### Associations

  Ecto supports defining associations on schemas:

      defmodule Post do
        use Ecto.Model

        @primary_key {:id, :binary_id, autogenerate: true}
        @foreign_key_type :binary_id

        schema "posts" do
          has_many :comments, Comment
        end
      end

      defmodule Comment do
        use Ecto.Model

        @primary_key {:id, :binary_id, autogenerate: true}
        @foreign_key_type :binary_id

        schema "comments" do
          field :title, :string
          belongs_to :post, Post
        end
      end

  Once an association is defined, Ecto provides a couple conveniences. The
  first one is the `Ecto.Model.assoc/2` function that allows us to easily
  retrieve all associated data to a given struct:

      import Ecto.Model

      # Get all comments for the given post
      Repo.all assoc(post, :comments)

      # Or build a query on top of the associated comments
      query = from c in assoc(post, :comments), where: c.title != nil
      Repo.all(query)

  When an association is defined, Ecto also defines a field in the model
  with the association name. By default, associations are not loaded into
  this field:

      iex> post = Repo.get(Post, 42)
      iex> post.comments
      #Ecto.Association.NotLoaded<...>

  However, developers can use the preload functionality in queries to
  automatically pre-populate the field:

      iex> post = Repo.get from(p in Post, preload: [:comments]), 42
      iex> post.comments
      [%Comment{...}, %Comment{...}]

  Keep in mind that MongoDB does not support joins as used in SQL - it's not
  possible to query your associations together with the main model. If you find
  yourself doing that you may need to consider using a `:map` field with
  embedded objects.

  Some more elaborate association schemas may force Ecto to use joins in
  some queries, that are not supported by MongoDB.
  One such call is `Ecto.Model.assoc/2` function combined with a `through` relation.

  You can find more information about defining associations and each respective
  association module in `Ecto.Schema` docs.

  > NOTE: Ecto does not lazy load associations. While lazily loading associations
  > may sound convenient at first, in the long run it becomes a source of confusion
  > and performance issues.

  ### Migrations

  TODO: Actually support migrations

  Ecto supports database migrations. You can generate a migration with:
      $ mix ecto.gen.migration create_posts

  This will create a new file inside `priv/repo/migrations` with the `up` and
  `down` functions. Check `Ecto.Migration` for more information.

  Keep in mind that MongoDB does not support (or need) database schemas, so
  majority of the functionality provided by `Ecto.Migration` is not usefull when
  working with MongoDB. What is very usefull are index definitions.

  ## MongoDB adapter features

  The adapter uses `mongodb` for communicating with the database and a pooling library
  such as `poolboy` for managing connections.

  The adapter has:

    * Support for documents with ObjectID as their primary key
    * Support for insert, find, update and remove mongo functions
    * Support for management commands with `command/2`
    * Support for embedded objects with `:map` and `{:array, :map}` types

  ## MongoDB adapter options

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
