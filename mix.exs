defmodule Mongo.Ecto.MixProject do
  use Mix.Project

  @version "0.3.0-dev"

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

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mongodb, "~> 0.4.7"},
      {:ecto, "~> 3.0.5"},
      {:excoveralls, "0.10.3", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end

  defp description do
    "MongoDB adapter for Ecto"
  end

  defp package do
    [
      maintainers: ["Justin Wood"],
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
