# Mongo.Ecto

![CI](https://github.com/elixir-mongo/mongodb_ecto/actions/workflows/ci.yml/badge.svg)
[![Hex.pm](https://img.shields.io/hexpm/v/mongodb_ecto.svg)](https://hex.pm/packages/mongodb_ecto)
[![Module Version](https://img.shields.io/hexpm/v/mongodb_ecto.svg)](https://hex.pm/packages/mongodb_ecto)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/mongodb_ecto/)
[![Total Download](https://img.shields.io/hexpm/dt/mongodb_ecto.svg)](https://hex.pm/packages/mongodb_ecto)
[![License](https://img.shields.io/hexpm/l/mongodb_ecto.svg)](https://github.com/elixir-mongo/mongodb_ecto/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/elixir-mongo/mongodb_ecto.svg)](https://github.com/elixir-mongo/mongodb_ecto/commits/master)

`Mongo.Ecto` is a MongoDB adapter for Ecto.

For detailed information read the documentation for the `Mongo.Ecto` module,
or check out examples below.

## Example

```elixir
# In your config/config.exs file
config :my_app, Repo,
  adapter: Mongo.Ecto,
  # Example key-value configuration:
  database: "ecto_simple",
  username: "mongodb",
  password: "mongodb",
  hostname: "localhost"
  # OR you can configure with a connection string (mongodb:// or mongodb+srv://):
  mongo_url: "mongodb://mongodb:mongodb@localhost:27017/ecto_simple"

config :my_app,
  # Add Repo to this list so you can run commands like `mix ecto.create`.
  ecto_repos: [Repo]

# In your application code
defmodule Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Mongo.Ecto

  def pool() do
    Ecto.Adapter.lookup_meta(__MODULE__).pid
  end
end

defmodule Weather do
  use Ecto.Schema

  # see Mongo.Ecto module docs for explanation of this line
  @primary_key {:id, :binary_id, autogenerate: true}

  # weather is the MongoDB collection name
  schema "weather" do
    field :city     # Defaults to type :string
    field :temp_lo, :integer
    field :temp_hi, :integer
    field :prcp,    :float, default: 0.0
  end
end

defmodule Simple do
  import Ecto.Query

  def sample_query do
    query = from w in Weather,
          where: w.prcp > 0 or is_nil(w.prcp),
         select: w
    Repo.all(query)
  end
end
```

## Usage

Add `:mongodb_ecto` as a dependency in your `mix.exs` file.

```elixir
def deps do
  [
    {:mongodb_ecto, "~> 2.1.1"}
  ]
end
```

To use the adapter in your repo:

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Mongo.Ecto
end
```

For additional information on usage please see the documentation for the [Mongo.Ecto module](https://hexdocs.pm/mongodb_ecto/Mongo.Ecto.html) and for [Ecto](http://hexdocs.pm/ecto).

## Data Type Mapping

| BSON               | Ecto                    |
| ------------------ | ----------------------- |
| double             | `:float`                |
| string             | `:string`               |
| object             | `:map`                  |
| array              | `{:array, subtype}`     |
| binary data        | `:binary`               |
| binary data (uuid) | `Ecto.UUID`             |
| object id          | `:binary_id`            |
| boolean            | `:boolean`              |
| date               | `Ecto.DateTime`         |
| regular expression | `Mongo.Ecto.Regex`      |
| JavaScript         | `Mongo.Ecto.JavaScript` |
| symbol             | (see below)             |
| 32-bit integer     | `:integer`              |
| timestamp          | `BSON.Timestamp`        |
| 64-bit integer     | `:integer`              |

Symbols are deprecated by the
[BSON specification](http://bsonspec.org/spec.html). They will be converted
to simple strings on reads. There is no possibility of persisting them to
the database.

Additionally special values are translated as follows:

| BSON    | Ecto        |
| ------- | ----------- |
| null    | `nil`       |
| min key | `:BSON_min` |
| max key | `:BSON_max` |

## Supported Mongo versions

The adapter and the driver are tested against most recent versions from 5.0, 6.0, and 7.0.

## Migrating to 2.0

Release 2.0 changes the underlying driver from [`mongodb`](https://github.com/elixir-mongo/mongodb) to [`mongodb_driver`](https://github.com/zookzook/elixir-mongodb-driver) 1.4. Calls to the Ecto adapter itself should not require any changes. Some config options are no longer used and can be simply deleted: `pool`, `pool_overflow`, `pool_timeout`.

If you make direct calls to the `Mongo` driver, you will need to update some of them to account for the `mongodb` -> `mongodb_driver` upgrade. Also, remember to replace `:mongodb` with `{:mongodb_driver, "~> 1.4"}` in your `mix.exs`. The known updates are:

1. `Mongo` functions no longer accept a `pool` option or `MyApp.Repo.Pool` module argument. Instead, a pool PID is expected:

   ```elixir
   # Old driver call
   Mongo.find(MyApp.Repo.Pool, "my_coll", %{"id": id}, projection: %{"field": 1}, pool: db_pool())

   # New driver call
   Mongo.find(MyApp.Repo.pool(), "my_coll", %{"id": id}, projection: %{"field": 1})

   # repo.ex
   # Provided the following function is defined in MyApp.Repo:
   defmodule MyApp.Repo do
     use Ecto.Repo, otp_app: :my_app, adapter: Mongo.Ecto

     def pool() do
       Ecto.Adapter.lookup_meta(__MODULE__).pid
     end
   end
   ```

2. [`Mongo.command`](https://hexdocs.pm/mongodb_driver/1.4.1/Mongo.html#command/3) requires a keyword list instead of a document. E.g., instead of `Mongo.command(MyApp.Repo.pool(), %{listCollections: 1}, opts)`, do `Mongo.command(MyApp.Repo.pool(), [listCollections: 1], opts)`.
3. `Mongo.ReadPreferences.defaults` is renamed to `Mongo.ReadPreference.merge_defaults`.
4. When passing a `hint` to `Mongo.find_one` etc., if the hinted index does not exist, an error is now returned.

## Contributing

To contribute you need to compile `Mongo.Ecto` from source and test it:

```
$ git clone https://github.com/ankhers/mongodb_ecto.git
$ cd mongodb_ecto
$ mix test
```

## Copyright and License

Copyright 2015 Michał Muskała

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
