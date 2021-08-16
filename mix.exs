defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  @version "0.2.1"

  def project do
    [
      app: :mongodb_ecto,
      version: @version,
      elixir: "~> 1.4",
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      description: description(),
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [applications: [:ecto, :mongodb, :logger, :telemetry]]
  end

  defp deps do
    [
      {:mongodb, github: "elixir-ecto/mongodb", branch: "ecto-3"},
      {:ecto, "~> 3.6"},
      {:dialyxir, "~> 1.1.0", only: :dev, runtime: false},
      {:excoveralls, "~> 0.8", only: :test},
      {:inch_ex, "~> 2.0.0", only: [:dev, :test]},
      {:earmark, "~> 1.0", only: :dev},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:poolboy, ">= 1.5.0", only: [:dev, :test]},
      {:telemetry, ">= 0.4.0"}
    ]
  end

  defp description do
    """
    MongoDB adapter for Ecto
    """
  end

  defp package do
    [
      maintainers: ["Michał Muskała", "Justin Wood", "Scott Ames-Messinger"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/commoncurriculum/mongodb_ecto"},
      files: ~w(mix.exs README.md CHANGELOG.md lib)
    ]
  end

  defp docs do
    [
      source_url: "https://github.com/commoncurriuclum/mongodb_ecto",
      source_ref: "v#{@version}",
      main: "readme",
      extras: ["README.md"]
    ]
  end
end
