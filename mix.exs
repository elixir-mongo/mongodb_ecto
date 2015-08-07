defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :mongodb_ecto,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps,
     test_coverage: [tool: ExCoveralls]]
  end

  def application do
    [applications: [:ecto, :mongodb]]
  end

  defp deps do
    [
      {:mongodb, github: "ericmj/mongodb", ref: "97e96a8de6f549d6fc42fad5666ecb253cdc29bf"},
      {:ecto, "~> 0.15.0"},
      {:inch_ex, only: :docs},
      {:dialyze, "~> 0.2.0", only: :dev},
      {:excoveralls, "~> 0.3.11", only: :test}
    ]
  end
end
