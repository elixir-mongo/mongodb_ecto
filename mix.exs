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
      {:ecto, github: "elixir-lang/ecto", ref: "a22c5ddd4cbd551b11635fe48f3d51e56afce806"},
      {:inch_ex, only: :docs},
      {:dialyze, "~> 0.2.0", only: :dev}
    ]
  end
end
