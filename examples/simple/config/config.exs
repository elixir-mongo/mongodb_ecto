use Mix.Config

config :simple, Simple.Repo,
  adapter: Mongo.Ecto,
  database: "ecto_simple",
  username: "mongodb",
  password: "mongodb",
  hostname: "localhost"
