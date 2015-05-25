defmodule MongodbEcto.Mixfile do
  use Mix.Project

  def project do
    [app: :mongodb_ecto,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps(Mix.env)]
  end

  def application do
    [applications: [:ecto, :mongodb]]
  end

  defp deps do
    [
        {:mongodb, github: "comtihon/mongodb-erlang"}
    ]
  end

  defp deps(:test) do
    [
        {:ecto, github: "elixir-lang/ecto"}
    ] ++ deps
  end

  defp deps(_) do
    [
        {:ecto, "~> 0.11"}
    ] ++ deps
  end
end
