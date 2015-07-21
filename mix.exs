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
      {:ecto, github: "elixir-lang/ecto", ref: "a19072ba9ca1ffa0b440578a7e345bf9a582f344"}
    ]
  end
end
