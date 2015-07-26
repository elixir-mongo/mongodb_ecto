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
      {:mongodb, github: "ericmj/mongodb", ref: "a379e577f51f8c9190ab22234a2512577e061e91"},
      {:ecto, github: "elixir-lang/ecto", ref: "9f5b5eb493bee23b6c4482f3da01e42f920ac770"},
      {:inch_ex, only: :docs}
    ]
  end
end
