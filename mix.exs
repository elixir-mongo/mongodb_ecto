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
      docs: docs(),
      dialyzer: dialyzer()
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
      {:earmark, "~> 1.0", only: :dev},
      {:ecto, "~> 3.6"},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:excoveralls, "~> 0.8", only: :test},
      {:inch_ex, "~> 2.0.0", only: [:dev, :test]},
      {:mongodb, github: "commoncurriculum/mongodb", branch: "ecto-3"},
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

  # Configures dialyzer (static analysis tool for Elixir / Erlang).
  #
  # The `dialyzer.plt` file takes a long time to generate first time round, so we store it in a
  # custom location where it can then be easily cached during CI.
  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
    ]
  end
end
