defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  @version "0.1.4"

  def project do
    [app: :mongodb_ecto,
     version: @version,
     elixir: "~> 1.2",
     deps: deps(),
     test_coverage: [tool: ExCoveralls],
     description: description(),
     package: package(),
     docs: docs()]
  end

  def application do
    [applications: [:ecto, :mongodb, :logger]]
  end

  defp deps do
    [
      {:mongodb, "~> 0.2.0"},
      {:ecto, "~> 2.0.0", github: "elixir-ecto/ecto", ref: "v2.0"},
      {:dialyze, "~> 0.2.0", only: :dev},
      {:inch_ex, "~> 0.5", only: :docs},
      {:earmark, "~> 1.0", only: :docs},
      {:ex_doc, "~> 0.11", only: :docs}
    ]
  end

  defp description do
    """
    MongoDB adapter for Ecto
    """
  end

  defp package do
    [maintainers: ["Michał Muskała"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/michalmuskala/mongodb_ecto"},
     files: ~w(mix.exs README.md CHANGELOG.md lib)]
  end

  defp docs do
    [source_url: "https://github.com/michalmuskala/mongodb_ecto",
     source_ref: "v#{@version}",
     main: "readme",
     extras: ["README.md"]]
  end
end
