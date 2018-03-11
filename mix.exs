defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  @version "0.1.4"

  def project do
    [
      app: :mongodb_ecto,
      version: @version,
      elixir: "~> 1.3",
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [applications: [:ecto, :mongodb, :logger]]
  end

  defp deps do
    [
      {:mongodb, "~> 0.4.2"},
      {:ecto, "~> 2.1.0"},
      {:dialyxir, "~> 0.5", only: :dev, runtime: false},
      {:excoveralls, "~> 0.8", only: :test},
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
    [
      maintainers: ["Michał Muskała", "Justin Wood"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/ankhers/mongodb_ecto"},
      files: ~w(mix.exs README.md CHANGELOG.md lib)
    ]
  end

  defp docs do
    [
      source_url: "https://github.com/ankhers/mongodb_ecto",
      source_ref: "v#{@version}",
      main: "readme",
      extras: ["README.md"]
    ]
  end
end
