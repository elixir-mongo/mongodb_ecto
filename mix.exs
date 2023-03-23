defmodule Mongo.Ecto.Mixfile do
  use Mix.Project

  @source_url "https://github.com/elixir-mongo/mongodb_ecto"
  @version "1.0.0"

  def project do
    [
      app: :mongodb_ecto,
      version: @version,
      elixir: "~> 1.7",
      deps: deps(),
      dialyzer: dialyzer(),
      docs: docs(),
      package: package(),
      preferred_cli_env: [docs: :docs],
      test_coverage: [tool: ExCoveralls]
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [applications: [:ecto, :mongodb, :logger, :telemetry]]
  end

  defp deps do
    [
      {:credo, "~> 1.5.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.1.0", only: :dev, runtime: false},
      {:ecto, "~> 3.6"},
      {:ex_doc, ">= 0.0.0", only: :docs, runtime: false},
      {:excoveralls, "~> 0.16", only: :test},
      {:mongodb, "~> 1.0.0"},
      {:telemetry, ">= 0.4.0"}
    ]
  end

  defp package do
    [
      description: "MongoDB adapter for Ecto",
      maintainers: [
        "Michał Muskała",
        "Justin Wood",
        "Scott Ames-Messinger",
        "Joe Pearson"
      ],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/elixir-mongo/mongodb_ecto"},
      files: ~w(mix.exs README.md CHANGELOG.md lib)
    ]
  end

  defp docs do
    [
      extras: [
        "CHANGELOG.md": [],
        LICENSE: [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end

  # Configures dialyzer (static analysis tool for Elixir / Erlang).
  #
  # The `dialyzer.plt` file takes a long time to generate first time round, so
  # we store it in a custom location where it can then be easily cached during
  # CI.
  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
    ]
  end
end
