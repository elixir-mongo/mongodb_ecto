defmodule Simple.Mixfile do
  use Mix.Project

  def project do
    [app: :embedded_models,
     version: "0.0.1",
     deps: deps]
  end

  def application do
    [mod: {EmbeddedModels.App, []},
     applications: [:mongodb_ecto, :ecto]]
  end

  defp deps do
    [{:mongodb_ecto, path: "../.."},
     {:ecto, path: "../../deps/ecto", override: true},
     {:mongodb, path: "../../deps/mongodb", override: true}]
  end
end
