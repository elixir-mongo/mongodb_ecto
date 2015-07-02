use Mix.Config

config :simple, Simple.Repo,
  adapter: Mongo.Ecto,
  database: "ecto_simple",
  hostname: "localhost"
