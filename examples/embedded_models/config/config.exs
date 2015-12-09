use Mix.Config

config :embedded_models, EmbeddedModels.Repo,
  adapter: Mongo.Ecto,
  database: "ecto_simple",
  hostname: "localhost"
