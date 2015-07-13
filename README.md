# Mongo.Ecto

`Mongo.Ecto` is a MongoDB adapter for Ecto.

For detailed information read the documentation for the `Mongo.Ecto` module,
or check out examples below.

## Example
```elixir
# In your config/config.exs file
config :my_app, Repo,
  database: "ecto_simple",
  username: "mongodb",
  password: "mongosb",
  hostname: "localhost"

# In your application code
defmodule Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Mongo.Ecto
end

defmodule Weather do
  use Ecto.Model

  @primary_key {:id, :binary_id, autogenerate: true}
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

Add Mongo.Ecto as a dependency in your `mix.exs` file.
```elixir
def deps do
  [{:mongodb_ecto, github: "michalmuskala/mongodb_ecto"}]
end
```

You should also update your applications to include both projects.
```elixir
def application do
  [applications: [:logger, :mongodb_ecto, :ecto]]
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

For additional information on usage please see the documentation for [Ecto](http://hexdocs.pm/ecto).

## Data Type Mapping

	BSON             	Ecto
	----------        	------
    double              :float
    string              :string
    object              :map
    array               {:array, subtype}
    binary data         :binary
    binary data (uuid)  Ecto.UUID
    object id           :binary_id
    boolean             :boolean
    date                :datetime
    regular expression  Mongo.Ecto.Regex
    JavaScript          Mongo.Ecto.JavaScript
    symbol              ???
    32-bit integer      :integer
    timestamp           BSON.Timestamp
    64-bit integer      :integer

Additionally special values are translated as follows:

	BSON             	Ecto
	----------        	------
    null                nil
    min key             :BSON_min
    max key             :BSON_max

## Supported Mongo versions

The adapter and the driver are tested against most recent versions from 3
branches: 2.4.x, 2.6.x, 3.0.x

## Contributing

To contribute you need to compile `Mongo.Ecto` from source and test it:

```
$ git clone https://github.com/michalmuskala/mongodb_ecto.git
$ cd mongodb_ecto
$ mix test
```

## License

Copyright 2015 Michał Muskała

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
