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
      {:mongodb, github: "ericmj/mongodb"},
      {:ecto, github: "elixir-lang/ecto"}
    ]
  end
end
