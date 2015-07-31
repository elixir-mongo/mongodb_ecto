defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :mongodb_ecto,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps]
  end

  def application do
    [applications: [:ecto, :mongodb]]
  end

  defp deps do
    [
      {:mongodb, github: "ericmj/mongodb", ref: "97e96a8de6f549d6fc42fad5666ecb253cdc29bf"},
      {:ecto, github: "elixir-lang/ecto", ref: "9f5b5eb493bee23b6c4482f3da01e42f920ac770"},
      {:inch_ex, only: :docs},
      {:dialyze, "~> 0.2.0", only: :dev}
    ]
  end
end
