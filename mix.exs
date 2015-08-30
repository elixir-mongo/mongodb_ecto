defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  @version "0.1.1"

  def project do
    [app: :mongodb_ecto,
     version: @version,
     elixir: "~> 1.0",
     deps: deps,
     test_coverage: [tool: ExCoveralls],
     description: description,
     package: package,
     docs: docs]
  end

  def application do
    [applications: [:ecto, :mongodb]]
  end

  defp deps do
    [
      {:mongodb, "~> 0.1"},
      {:ecto, "~> 1.0"},
      {:dialyze, "~> 0.2.0", only: :dev},
      {:excoveralls, "~> 0.3.11", only: :test},
      {:inch_ex, only: :docs},
      {:earmark, "~> 0.1", only: :docs},
      {:ex_doc, "~> 0.8", only: :docs}
    ]
  end

  defp description do
    """
    MongoDB adapter for Ecto
    """
  end

  defp package do
    [contributors: ["Michał Muskała"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/michalmuskala/mongodb_ecto"},
     files: ~w(mix.exs README.md CHANGELOG.md lib)]
  end

  defp docs do
    [readme: "README.md",
     source_url: "https://github.com/michalmuskala/mongodb_ecto",
     source_ref: "v#{@version}",
     main: "README"]
  end
end
