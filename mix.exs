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
      {:mongodb, "~> 0.1"},
      {:ecto, "~> 0.16"},
      {:inch_ex, only: :docs},
      {:dialyze, "~> 0.2.0", only: :dev},
      {:excoveralls, "~> 0.3.11", only: :test}
    ]
  end
end
